/*!
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import axios, { AxiosResponse } from 'axios';
import { useQuery } from 'react-query';
import ELK, { ElkShape, ElkExtendedEdge } from 'elkjs';

import { getMetaValue } from 'src/utils';
import type { DepEdge, DepNode } from 'src/types';
import type { NodeType } from 'src/datasets/Graph/Node';
import { unionBy } from 'lodash';

interface DatasetDependencies {
  edges: DepEdge[];
  nodes: DepNode[];
}

interface GenerateProps {
  nodes: DepNode[];
  edges: DepEdge[];
  font: string;
}

interface Data extends ElkShape {
  children: NodeType[];
  edges: ElkExtendedEdge[];
}

// Take text and font to calculate how long each node should be
function getTextWidth(text: string, font: string) {
  const context = document.createElement('canvas').getContext('2d');
  if (context) {
    context.font = font;
    const metrics = context.measureText(text);
    return metrics.width;
  }
  return text.length * 9;
}

const generateGraph = ({ nodes, edges, font }: GenerateProps) => ({
  id: 'root',
  layoutOptions: {
    'spacing.nodeNodeBetweenLayers': '40.0',
    'spacing.edgeNodeBetweenLayers': '10.0',
    'layering.strategy': 'INTERACTIVE',
    algorithm: 'layered',
    'spacing.edgeEdgeBetweenLayers': '10.0',
    'spacing.edgeNode': '10.0',
    'spacing.edgeEdge': '10.0',
    'spacing.nodeNode': '20.0',
    'elk.direction': 'DOWN',
  },
  children: nodes.map(({ id, value }) => ({
    id,
    // calculate text width and add space for padding/icon
    width: getTextWidth(value.label, font) + 36,
    height: 40,
    value,
  })),
  edges: edges.map((e) => ({ id: `${e.source}-${e.target}`, sources: [e.source], targets: [e.target] })),
});

interface SeparateGraphsProps extends DatasetDependencies {
  graphs: DatasetDependencies[];
}

const graphIndicesToMerge: Record<number, number[]> = {};

const findDownstreamGraph = (
  { edges, nodes, graphs = [] }: SeparateGraphsProps,
): DatasetDependencies[] => {
  const newGraphs = [...graphs];
  let filteredEdges = [...edges];

  graphs.forEach((g, i) => {
    const newEdges = edges.filter((e) => g.edges.some((ge) => ge.target === e.source));
    const newNodes: DepNode[] = [];
    newEdges.forEach((e) => {
      const newNode = nodes.find((n) => n.id === e.target);
      if (newNode) {
        newNodes.push(newNode);
        const existingGraphIndex = newGraphs
          .findIndex(((ng) => ng.nodes.some((n) => n.id === newNode.id)));
        if (existingGraphIndex > -1) {
          graphIndicesToMerge[i] = [...(graphIndicesToMerge[i] || []), existingGraphIndex];
        }
        newGraphs[i] = {
          nodes: [...newGraphs[i].nodes, newNode],
          edges: [...newGraphs[i].edges, e],
        };
        filteredEdges = filteredEdges
          .filter((fe) => fe.source !== e.source && fe.target !== e.target);
      }
    });
  });

  if (!filteredEdges.length) {
    const mergedGraphs: DatasetDependencies[] = [...newGraphs];
    Object.keys(graphIndicesToMerge).forEach((key) => {
      const realKey = key as unknown as number;
      const values = graphIndicesToMerge[realKey];
      values.forEach((v) => {
        mergedGraphs[realKey] = {
          nodes: unionBy(mergedGraphs[realKey].nodes, newGraphs[v as unknown as number].nodes, 'id'),
          edges: [...mergedGraphs[realKey].edges, ...newGraphs[v as unknown as number].edges],
        };
      });
    });

    Object.keys(graphIndicesToMerge).forEach((key) => {
      const realKey = key as unknown as number;
      const values = graphIndicesToMerge[realKey];
      values.reverse().forEach((v) => {
        mergedGraphs.splice(v, 1);
      });
    });
    return mergedGraphs;
  }
  return findDownstreamGraph({ edges: filteredEdges, nodes, graphs: newGraphs });
};

const separateGraphs = ({ edges, nodes }: DatasetDependencies): DatasetDependencies[] => {
  const newGraphs: DatasetDependencies[] = [];
  let remainingEdges = [...edges];
  let remainingNodes = [...nodes];

  edges.forEach((edge) => {
    const downstreams = edges.filter((e) => e.target === edge.source);
    if (!downstreams.length) {
      const newNodes = nodes.filter((n) => n.id === edge.source || n.id === edge.target);
      const nodesInUse = newGraphs
        .findIndex((g) => g.nodes.some((n) => newNodes.some((nn) => nn.id === n.id)));
      if (nodesInUse > -1) {
        const { nodes: existingNodes, edges: existingEdges } = newGraphs[nodesInUse];
        newGraphs[nodesInUse] = { nodes: unionBy(existingNodes, newNodes, 'id'), edges: [...existingEdges, edge] };
      } else {
        newGraphs.push({ nodes: newNodes, edges: [edge] });
      }
      remainingEdges = remainingEdges.filter((e) => e.source !== edge.source);
      remainingNodes = remainingNodes.filter((n) => !newNodes.some((nn) => nn.id === n.id));
    }
  });

  if (remainingEdges.length) {
    return findDownstreamGraph({ edges: remainingEdges, nodes, graphs: newGraphs });
  }
  return newGraphs;
};

const formatDependencies = async ({ edges, nodes }: DatasetDependencies) => {
  const elk = new ELK();

  const graphs = separateGraphs({ edges, nodes });

  // get computed style to calculate how large each node should be
  const font = `bold ${16}px ${window.getComputedStyle(document.body).fontFamily}`;

  // Finally generate the graph data with elk
  const data = await Promise.all(graphs.map(async (g) => (
    elk.layout(generateGraph({ nodes: g.nodes, edges: g.edges, font }))
  )));
  return data as Data[];
};

export default function useDatasetDependencies() {
  return useQuery(
    'datasetDependencies',
    async () => {
      const datasetDepsUrl = getMetaValue('dataset_dependencies_url');
      const rawData = await axios.get<AxiosResponse, DatasetDependencies>(datasetDepsUrl);
      return formatDependencies(rawData);
    },
  );
}
