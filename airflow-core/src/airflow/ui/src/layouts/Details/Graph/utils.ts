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
import type { Edge } from "@xyflow/react";

type FilterType = "all" | "both" | "downstream" | "upstream";

type GraphParams<T extends { id: string }> = {
  edges: Array<Edge>;
  filter: FilterType;
  nodes: Array<T>;
  taskId?: string;
};

export const filterGraph = <T extends { id: string }>({
  edges,
  filter,
  nodes,
  taskId,
}: GraphParams<T>): { filteredEdges: Array<Edge>; filteredNodes: Array<T> } => {
  let filteredNodes = nodes;
  let filteredEdges = edges;

  const getUpstreamNodes = (rootTaskId: string, edgesList: Array<Edge>): Set<string> => {
    const visited = new Set<string>();
    const stack: Array<string> = [rootTaskId];

    while (stack.length > 0) {
      const current = stack.pop();

      if (current === undefined) {
        // eslint-disable-next-line no-continue
        continue;
      }

      visited.add(current);
      for (const edge of edgesList) {
        if (edge.target === current && !visited.has(edge.source)) {
          stack.push(edge.source);
        }
      }
    }

    return visited;
  };

  const getDownstreamNodes = (rootTaskId: string, edgesList: Array<Edge>): Set<string> => {
    const visited = new Set<string>();
    const stack: Array<string> = [rootTaskId];

    while (stack.length > 0) {
      const current = stack.pop();

      if (current === undefined) {
        // eslint-disable-next-line no-continue
        continue;
      }

      visited.add(current);
      for (const edge of edgesList) {
        if (edge.source === current && !visited.has(edge.target)) {
          stack.push(edge.target);
        }
      }
    }

    return visited;
  };

  if (taskId !== undefined && filter !== "all") {
    if (filter === "upstream") {
      const upstream = getUpstreamNodes(taskId, edges);

      filteredNodes = nodes.filter((node) => upstream.has(node.id));
      filteredEdges = edges.filter((edge) => upstream.has(edge.source) && upstream.has(edge.target));
    } else if (filter === "downstream") {
      const downstream = getDownstreamNodes(taskId, edges);

      filteredNodes = nodes.filter((node) => downstream.has(node.id));
      filteredEdges = edges.filter((edge) => downstream.has(edge.source) && downstream.has(edge.target));
    } else {
      // "both"
      const upstream = getUpstreamNodes(taskId, edges);
      const downstream = getDownstreamNodes(taskId, edges);
      const combined = new Set<string>([taskId, ...upstream, ...downstream]);

      filteredNodes = nodes.filter((node) => combined.has(node.id));
      filteredEdges = edges.filter((edge) => combined.has(edge.source) && combined.has(edge.target));
    }
  }

  return { filteredEdges, filteredNodes };
};
