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
export type LineageDirection = "downstream" | "upstream";

type AssetLineageEdge = {
  source_id: string;
  target_id: string;
};

const addNeighbor = (graph: Map<string, Set<string>>, nodeId: string, neighborId: string) => {
  const neighbors = graph.get(nodeId);

  if (neighbors === undefined) {
    graph.set(nodeId, new Set([neighborId]));

    return;
  }

  neighbors.add(neighborId);
};

const traverseLineage = ({
  adjacencyMap,
  edgeIdGenerator,
  rootNodeId,
}: {
  adjacencyMap: Map<string, Set<string>>;
  edgeIdGenerator: (currentNodeId: string, neighborNodeId: string) => string;
  rootNodeId: string;
}) => {
  const highlightedNodeIds = new Set<string>();
  const highlightedEdgeIds = new Set<string>();
  const queue = [rootNodeId];

  while (queue.length > 0) {
    const currentNodeId = queue.shift();

    if (currentNodeId !== undefined) {
      const neighbors = adjacencyMap.get(currentNodeId) ?? new Set<string>();

      neighbors.forEach((neighborNodeId) => {
        highlightedEdgeIds.add(edgeIdGenerator(currentNodeId, neighborNodeId));

        if (!highlightedNodeIds.has(neighborNodeId)) {
          highlightedNodeIds.add(neighborNodeId);
          queue.push(neighborNodeId);
        }
      });
    }
  }

  return { highlightedEdgeIds, highlightedNodeIds };
};

export const getHighlightedLineage = ({
  direction,
  edges,
  nodeId,
}: {
  direction: LineageDirection;
  edges: Array<AssetLineageEdge>;
  nodeId?: string;
}) => {
  if (nodeId === undefined) {
    return {
      highlightedEdgeIds: new Set<string>(),
      highlightedNodeIds: new Set<string>(),
    };
  }

  const incomingEdgesMap = new Map<string, Set<string>>();
  const outgoingEdgesMap = new Map<string, Set<string>>();

  edges.forEach((edge) => {
    const sourceId = edge.source_id;
    const targetId = edge.target_id;

    addNeighbor(incomingEdgesMap, targetId, sourceId);
    addNeighbor(outgoingEdgesMap, sourceId, targetId);
  });

  const upstreamHighlights =
    direction === "upstream"
      ? traverseLineage({
          adjacencyMap: incomingEdgesMap,
          edgeIdGenerator: (currentNodeId, upstreamNodeId) => `${upstreamNodeId}-${currentNodeId}`,
          rootNodeId: nodeId,
        })
      : { highlightedEdgeIds: new Set<string>(), highlightedNodeIds: new Set<string>() };
  const downstreamHighlights =
    direction === "downstream"
      ? traverseLineage({
          adjacencyMap: outgoingEdgesMap,
          edgeIdGenerator: (currentNodeId, downstreamNodeId) => `${currentNodeId}-${downstreamNodeId}`,
          rootNodeId: nodeId,
        })
      : { highlightedEdgeIds: new Set<string>(), highlightedNodeIds: new Set<string>() };

  return {
    highlightedEdgeIds: new Set([
      ...upstreamHighlights.highlightedEdgeIds,
      ...downstreamHighlights.highlightedEdgeIds,
    ]),
    highlightedNodeIds: new Set([
      nodeId,
      ...upstreamHighlights.highlightedNodeIds,
      ...downstreamHighlights.highlightedNodeIds,
    ]),
  };
};
