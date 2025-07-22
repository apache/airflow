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
import type { GridNodeResponse } from "openapi/requests/types.gen";

/**
 * Finds a task node by its ID in a tree of nodes
 * @param nodes - Array of root nodes to search through
 * @param targetId - ID of the node to find
 * @returns The found node or undefined if not found
 */
export const getGroupTask = (
  nodes: Array<GridNodeResponse> | undefined,
  targetId: string | undefined,
): GridNodeResponse | undefined => {
  if (nodes === undefined || targetId === undefined || !nodes.length || !targetId) {
    return undefined;
  }

  const queue: Array<GridNodeResponse> = [...nodes];
  const [root] = targetId.split(".");

  while (queue.length > 0) {
    const node = queue.shift();

    if (node) {
      if (node.id === targetId) {
        return node;
      }

      if (node.children && node.children.length > 0) {
        const nextNode =
          node.id === root && targetId.includes(".")
            ? node.children.find((child) => child.id === targetId)
            : undefined;

        queue.unshift(...(nextNode ? [nextNode] : node.children));
      }
    }
  }

  return undefined;
};
