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
import type { GridNodeResponse, NodeResponse } from "openapi/requests/types.gen";

export type GridTask = {
  depth: number;
  isGroup?: boolean;
  isOpen?: boolean;
} & GridNodeResponse;

export const flattenNodes = (
  nodes: Array<GridNodeResponse> | undefined,
  openGroupIds: Array<string>,
  depth: number = 0,
) => {
  let flatNodes: Array<GridTask> = [];
  let allGroupIds: Array<string> = [];

  nodes?.forEach((node) => {
    if (node.children) {
      const { children, ...rest } = node;

      flatNodes.push({ ...rest, depth, isGroup: true, isOpen: openGroupIds.includes(node.id) });
      allGroupIds.push(node.id);

      const { allGroupIds: childGroupIds, flatNodes: childNodes } = flattenNodes(
        children,
        openGroupIds,
        depth + 1,
      );

      flatNodes = [...flatNodes, ...(openGroupIds.includes(node.id) ? childNodes : [])];
      allGroupIds = [...allGroupIds, ...childGroupIds];
    } else {
      flatNodes.push({ ...node, depth });
    }
  });

  return { allGroupIds, flatNodes };
};

export const flattenGraphNodes = (
  nodes: Array<NodeResponse>,
  depth: number = 0,
): { allGroupIds: Array<string> } => {
  let allGroupIds: Array<string> = [];

  nodes.forEach((node) => {
    if (node.children) {
      allGroupIds.push(node.id);

      const { allGroupIds: childGroupIds } = flattenGraphNodes(node.children, depth + 1);

      allGroupIds = [...allGroupIds, ...childGroupIds];
    }
  });

  return { allGroupIds };
};

export type Edge = { source: string; target: string };

export const buildEdges = (structureEdges: Array<{ source_id: string; target_id: string }>): Array<Edge> =>
  structureEdges.map((edge) => ({ source: edge.source_id, target: edge.target_id }));

export const buildAdjacencyMaps = (edges: Array<Edge>) => {
  const upstreamMap = new Map<string, Array<string>>();
  const downstreamMap = new Map<string, Array<string>>();

  edges.forEach((edge) => {
    const upstreamSources = upstreamMap.get(edge.target) ?? [];

    upstreamSources.push(edge.source);
    upstreamMap.set(edge.target, upstreamSources);

    const downstreamTargets = downstreamMap.get(edge.source) ?? [];

    downstreamTargets.push(edge.target);
    downstreamMap.set(edge.source, downstreamTargets);
  });

  return { downstreamMap, upstreamMap };
};

export const collect = (startId: string, map: Map<string, Array<string>>): Set<string> => {
  const seen = new Set<string>();
  const stack = [startId];

  while (stack.length) {
    const id = stack.pop();

    if (id !== undefined && !seen.has(id)) {
      seen.add(id);

      const next = map.get(id) ?? [];

      next.filter((nextId) => !seen.has(nextId)).forEach((nextId) => stack.push(nextId));
    }
  }

  return seen;
};

export const filterNodesByDirection = <T extends { id: string; isGroup?: boolean }>({
  edges,
  filter,
  flatNodes,
  taskId,
}: {
  edges: Array<Edge>;
  filter: "all" | "both" | "downstream" | "upstream";
  flatNodes: Array<T>;
  taskId?: string;
}): Array<T> => {
  if (taskId === undefined || filter === "all") {
    return flatNodes;
  }

  const { downstreamMap, upstreamMap } = buildAdjacencyMaps(edges);

  const upstreamIds =
    filter === "upstream" || filter === "both" ? collect(taskId, upstreamMap) : new Set<string>();
  const downstreamIds =
    filter === "downstream" || filter === "both" ? collect(taskId, downstreamMap) : new Set<string>();

  const combined = new Set<string>([taskId, ...upstreamIds, ...downstreamIds]);

  return flatNodes.filter((node) => (node.isGroup ?? false) || combined.has(node.id));
};
