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

import { useGridStructure } from "src/queries/useGridStructure";
import { getGroupTask } from "src/utils/groupTask";

/** The given Task Group's node from the cached grid structure (carries ``is_loop``, ``doc_md``). */
export const useLoopGroupNode = (groupId: string): GridNodeResponse | undefined => {
  const { data } = useGridStructure({ limit: 1 });

  return getGroupTask(data, groupId);
};

/** Whether the given Task Group is a looped group (``is_loop``), from the cached grid structure. */
export const useIsLoopGroup = (groupId: string): boolean => Boolean(useLoopGroupNode(groupId)?.is_loop);

/** Every loop declared in the Dag, so a filter can offer them by the name the author gave. */
/**
 * The loops enclosing `nodeId`, or every loop in the Dag when no node is named.
 *
 * Membership comes from the structure rather than the id, because a loop declared with
 * `prefix_group_id=False` does not name its members after itself.
 */
export const loopsForNode = (nodes: Array<GridNodeResponse> | undefined, nodeId?: string): Array<string> => {
  const declared: Array<string> = [];
  let enclosing: Array<string> | undefined;
  const walk = (list: Array<GridNodeResponse> | undefined, ancestors: Array<string>) => {
    for (const node of list ?? []) {
      const isLoop = node.is_loop === true;
      const scope = isLoop ? [...ancestors, node.id] : ancestors;

      if (isLoop) {
        declared.push(node.id);
      }
      if (node.id === nodeId) {
        enclosing = scope;
      }
      walk(node.children ?? undefined, scope);
    }
  };

  walk(nodes, []);

  return nodeId === undefined ? declared : (enclosing ?? []);
};

/**
 * Every task id inside `groupId`, including the group itself.
 *
 * Read from the structure rather than the id, because a group declared with
 * `prefix_group_id=False` does not name its children after itself.
 */
export const membersOfGroup = (nodes: Array<GridNodeResponse> | undefined, groupId: string): Set<string> => {
  const members = new Set<string>();
  const collect = (node: GridNodeResponse) => {
    members.add(node.id);
    for (const child of node.children ?? []) {
      collect(child);
    }
  };
  const find = (list: Array<GridNodeResponse> | undefined): boolean =>
    (list ?? []).some((node) => {
      if (node.id === groupId) {
        collect(node);

        return true;
      }

      return find(node.children ?? undefined);
    });

  find(nodes);

  return members;
};

export const useGroupMembers = (groupId?: string): Set<string> => {
  const { data } = useGridStructure({ limit: 1 });

  return groupId === undefined ? new Set<string>() : membersOfGroup(data, groupId);
};

export const useLoopGroupIds = (nodeId?: string): Array<string> => {
  const { data } = useGridStructure({ limit: 1 });

  return loopsForNode(data, nodeId);
};
