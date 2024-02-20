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
import { getTextWidth } from "src/utils/graph";

import type { NodeType, DepEdge, DepNode } from "src/types";

export interface DatasetDependencies {
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

export interface DatasetGraph extends ElkShape {
  children: NodeType[];
  edges: ElkExtendedEdge[];
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

  const otherIndexes: number[] = [];

  const mergedGraphs = graphs
    .reduce((newGraphs, graph) => {
      // Find all overlapping graphs where at least one edge in each graph has the same target node
      const otherGroups = newGraphs.filter((otherGroup, i) =>
        otherGroup.edges.some((otherEdge) => {
          if (graph.edges.some((edge) => edge.target === otherEdge.target)) {
            otherIndexes.push(i);
            return true;
          }
          return false;
        })
      );
      if (!otherGroups.length) {
        return [...newGraphs, graph];
      }

      // Merge the edges of every overlapping group
      const mergedEdges = otherGroups
        .reduce(
          (totalEdges, group) => [...totalEdges, ...group.edges],
          [...graph.edges]
        )
        .filter(
          (edge, edgeIndex, otherEdges) =>
            edgeIndex ===
            otherEdges.findIndex(
              (otherEdge) =>
                otherEdge.source === edge.source &&
                otherEdge.target === edge.target
            )
        );
      return [
        // filter out the merged graphs
        ...newGraphs.filter(
          (_, newGraphIndex) => !otherIndexes.includes(newGraphIndex)
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

  // get computed style to calculate how large each node should be
  const font = `bold ${16}px ${
    window.getComputedStyle(document.body).fontFamily
  }`;

  const graph = await elk.layout(generateGraph({ nodes, edges, font }));

  return graph as DatasetGraph;
};

export default function useDatasetDependencies() {
  return useQuery("datasetDependencies", async () => {
    const datasetDepsUrl = getMetaValue("dataset_dependencies_url");
    return axios.get<AxiosResponse, DatasetDependencies>(datasetDepsUrl);
  });
}

interface GraphsProps {
  dagIds?: string[];
  selectedUri: string | null;
}

export const useDatasetGraphs = ({ dagIds, selectedUri }: GraphsProps) => {
  const { data: datasetDependencies } = useDatasetDependencies();
  return useQuery(
    ["datasetGraphs", datasetDependencies, dagIds, selectedUri],
    () => {
      if (datasetDependencies) {
        let graph = datasetDependencies;
        const subGraphs = datasetDependencies
          ? separateGraphs(datasetDependencies)
          : [];

        // Filter by dataset URI takes precedence
        if (selectedUri) {
          graph =
            subGraphs.find((g) =>
              g.nodes.some((n) => n.value.label === selectedUri)
            ) || graph;
        } else if (dagIds?.length) {
          const filteredSubGraphs = subGraphs.filter((sg) =>
            dagIds.some((dagId) =>
              sg.nodes.some((c) => c.value.label === dagId)
            )
          );

          graph = filteredSubGraphs.reduce(
            (graphs, subGraph) => ({
              edges: [...graphs.edges, ...subGraph.edges],
              nodes: [...graphs.nodes, ...subGraph.nodes],
            }),
            { edges: [], nodes: [] }
          );
        }

        return formatDependencies(graph);
      }
      return undefined;
    }
  );
};
