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

import axios, { AxiosResponse } from "axios";
import { useQuery } from "react-query";
import ELK, { ElkShape, ElkExtendedEdge } from "elkjs";

import { getMetaValue } from "src/utils";
import type { DepEdge, DepNode } from "src/types";
import type { NodeType } from "src/datasets/Graph/Node";

import { getTextWidth } from "src/utils/graph";

interface DatasetDependencies {
  edges: DepEdge[];
  nodes: DepNode[];
}

interface EdgeGroup {
  edges: DepEdge[];
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

const generateGraph = ({ nodes, edges, font }: GenerateProps) => ({
  id: "root",
  layoutOptions: {
    "spacing.nodeNodeBetweenLayers": "40.0",
    "spacing.edgeNodeBetweenLayers": "10.0",
    "layering.strategy": "INTERACTIVE",
    algorithm: "layered",
    "crossingMinimization.semiInteractive": "true",
    "spacing.edgeEdgeBetweenLayers": "10.0",
    "spacing.edgeNode": "10.0",
    "spacing.edgeEdge": "10.0",
    "spacing.nodeNode": "20.0",
    "elk.direction": "DOWN",
  },
  children: nodes.map(({ id, value }) => ({
    id,
    // calculate text width and add space for padding/icon
    width: getTextWidth(value.label, font) + 36,
    height: 40,
    value,
  })),
  edges: edges.map((e) => ({
    id: `${e.source}-${e.target}`,
    sources: [e.source],
    targets: [e.target],
  })),
});

interface SeparateGraphsProps {
  edges: DepEdge[];
  graphs: EdgeGroup[];
}

// find the downstream graph of each upstream edge
const findDownstreamGraph = ({
  edges,
  graphs = [],
}: SeparateGraphsProps): EdgeGroup[] => {
  let unassignedEdges = [...edges];

  const mergedGraphs = graphs
    .reduce((newGraphs, graph) => {
      const otherGroupIndex = newGraphs.findIndex((otherGroup) =>
        otherGroup.edges.some((otherEdge) =>
          graph.edges.some((edge) => edge.target === otherEdge.target)
        )
      );
      if (otherGroupIndex === -1) {
        return [...newGraphs, graph];
      }

      const mergedEdges = [
        ...newGraphs[otherGroupIndex].edges,
        ...graph.edges,
      ].filter(
        (edge, edgeIndex, otherEdges) =>
          edgeIndex ===
          otherEdges.findIndex(
            (otherEdge) =>
              otherEdge.source === edge.source &&
              otherEdge.target === edge.target
          )
      );
      return [
        ...newGraphs.filter(
          (_, newGraphIndex) => newGraphIndex !== otherGroupIndex
        ),
        { edges: mergedEdges },
      ];
    }, [] as EdgeGroup[])
    .map((graph) => {
      // find the next set of downstream edges and filter them out of the unassigned edges list
      const downstreamEdges: DepEdge[] = [];
      unassignedEdges = unassignedEdges.filter((edge) => {
        const isDownstream = graph.edges.some(
          (graphEdge) => graphEdge.target === edge.source
        );
        if (isDownstream) downstreamEdges.push(edge);
        return !isDownstream;
      });

      return {
        edges: [...graph.edges, ...downstreamEdges],
      };
    });

  // recursively find downstream edges until there are no unassigned edges
  return unassignedEdges.length
    ? findDownstreamGraph({ edges: unassignedEdges, graphs: mergedGraphs })
    : mergedGraphs;
};

// separate the list of nodes/edges into distinct dataset pipeline graphs
const separateGraphs = ({
  edges,
  nodes,
}: DatasetDependencies): DatasetDependencies[] => {
  const separatedGraphs: EdgeGroup[] = [];
  const remainingEdges: DepEdge[] = [];

  edges.forEach((e) => {
    // add a separate graph for each edge without an upstream
    if (!edges.some((ee) => e.source === ee.target)) {
      separatedGraphs.push({ edges: [e] });
    } else {
      remainingEdges.push(e);
    }
  });

  const edgeGraphs = findDownstreamGraph({
    edges: remainingEdges,
    graphs: separatedGraphs,
  });

  // once all the edges are found, add the nodes
  return edgeGraphs.map((eg) => {
    const graphNodes = nodes.filter((n) =>
      eg.edges.some((e) => e.target === n.id || e.source === n.id)
    );
    return {
      edges: eg.edges,
      nodes: graphNodes,
    };
  });
};

const formatDependencies = async ({ edges, nodes }: DatasetDependencies) => {
  const elk = new ELK();

  const graphs = separateGraphs({ edges, nodes });

  // get computed style to calculate how large each node should be
  const font = `bold ${16}px ${
    window.getComputedStyle(document.body).fontFamily
  }`;

  // Finally generate the graph data with elk
  const subGraphs = await Promise.all(
    graphs.map(async (g) =>
      elk.layout(generateGraph({ nodes: g.nodes, edges: g.edges, font }))
    )
  );
  const fullGraph = await elk.layout(generateGraph({ nodes, edges, font }));

  return {
    fullGraph,
    subGraphs,
  } as Data;
};

export default function useDatasetDependencies() {
  return useQuery("datasetDependencies", async () => {
    const datasetDepsUrl = getMetaValue("dataset_dependencies_url");
    const rawData = await axios.get<AxiosResponse, DatasetDependencies>(
      datasetDepsUrl
    );
    return formatDependencies(rawData);
  });
}
