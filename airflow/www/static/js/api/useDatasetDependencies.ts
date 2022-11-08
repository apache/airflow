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

interface Graph extends ElkShape {
  children: NodeType[];
  edges: ElkExtendedEdge[];
}

interface Data {
  fullGraph: Graph;
  subGraphs: Graph[];
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
    'crossingMinimization.semiInteractive': 'true',
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
const indicesToRemove: number[] = [];

// find the downstream graph of each upstream edge
const findDownstreamGraph = (
  { edges, nodes, graphs = [] }: SeparateGraphsProps,
): DatasetDependencies[] => {
  const newGraphs = [...graphs];
  let filteredEdges = [...edges];

  graphs.forEach((g, i) => {
    // find downstream edges
    const downstreamEdges = edges.filter((e) => g.edges.some((ge) => ge.target === e.source));
    const downstreamNodes: DepNode[] = [];

    downstreamEdges.forEach((e) => {
      const newNode = nodes.find((n) => n.id === e.target);
      if (newNode) {
        downstreamNodes.push(newNode);

        // check if the node already exists in a different graph
        const existingGraphIndex = newGraphs
          .findIndex(((ng) => ng.nodes.some((n) => n.id === newNode.id)));

        // mark if the graph needs to merge with another
        if (existingGraphIndex > -1) {
          indicesToRemove.push(existingGraphIndex);
          graphIndicesToMerge[i] = [...(graphIndicesToMerge[i] || []), existingGraphIndex];
        }

        // add node and edge to the graph
        newGraphs[i] = {
          nodes: [...newGraphs[i].nodes, newNode],
          edges: [...newGraphs[i].edges, e],
        };

        // remove edge from edge list
        filteredEdges = filteredEdges
          .filter((fe) => !(fe.source === e.source && fe.target === e.target));
      }
    });
  });

  // once there are no more filtered edges left, merge relevant graphs
  // we merge afterwards to make sure we captured all nodes + edges
  if (!filteredEdges.length) {
    Object.keys(graphIndicesToMerge).forEach((key) => {
      const realKey = key as unknown as number;
      const values = graphIndicesToMerge[realKey];
      values.forEach((v) => {
        newGraphs[realKey] = {
          nodes: unionBy(newGraphs[realKey].nodes, newGraphs[v].nodes, 'id'),
          edges: [...newGraphs[realKey].edges, ...newGraphs[v].edges]
            .filter((e, i, s) => (
              i === s.findIndex((t) => t.source === e.source && t.target === e.target)
            )),
        };
      });
    });
    return newGraphs.filter((g, i) => !indicesToRemove.some((j) => i === j));
  }

  return findDownstreamGraph({ edges: filteredEdges, nodes, graphs: newGraphs });
};

// separate the list of nodes/edges into distinct dataset pipeline graphs
const separateGraphs = ({ edges, nodes }: DatasetDependencies): DatasetDependencies[] => {
  const separatedGraphs: DatasetDependencies[] = [];
  let remainingEdges = [...edges];
  let remainingNodes = [...nodes];

  edges.forEach((edge) => {
    const isDownstream = edges.some((e) => e.target === edge.source);

    // if the edge is not downstream of anything, then start building the graph
    if (!isDownstream) {
      const connectedNodes = nodes.filter((n) => n.id === edge.source || n.id === edge.target);

      // check if one of the nodes is already connected to a separated graph
      const nodesInUse = separatedGraphs
        .findIndex((g) => g.nodes.some((n) => connectedNodes.some((nn) => nn.id === n.id)));

      if (nodesInUse > -1) {
        // if one of the nodes is already in use, merge the graphs
        const { nodes: existingNodes, edges: existingEdges } = separatedGraphs[nodesInUse];
        separatedGraphs[nodesInUse] = { nodes: unionBy(existingNodes, connectedNodes, 'id'), edges: [...existingEdges, edge] };
      } else {
        // else just add the new separated graph
        separatedGraphs.push({ nodes: connectedNodes, edges: [edge] });
      }

      // filter out used nodes and edges
      remainingEdges = remainingEdges.filter((e) => e.source !== edge.source);
      remainingNodes = remainingNodes.filter((n) => !connectedNodes.some((nn) => nn.id === n.id));
    }
  });

  if (remainingEdges.length) {
    return findDownstreamGraph({ edges: remainingEdges, nodes, graphs: separatedGraphs });
  }
  return separatedGraphs;
};

const formatDependencies = async ({ edges, nodes }: DatasetDependencies) => {
  const elk = new ELK();

  const graphs = separateGraphs({ edges, nodes });

  // get computed style to calculate how large each node should be
  const font = `bold ${16}px ${window.getComputedStyle(document.body).fontFamily}`;

  // Finally generate the graph data with elk
  const subGraphs = await Promise.all(graphs.map(async (g) => (
    elk.layout(generateGraph({ nodes: g.nodes, edges: g.edges, font }))
  )));
  const fullGraph = await elk.layout(generateGraph({ nodes, edges, font }));
  return {
    fullGraph,
    subGraphs,
  } as Data;
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
