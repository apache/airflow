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
export const useLoopGroupIds = (): Array<string> => {
  const { data } = useGridStructure({ limit: 1 });
  const found: Array<string> = [];
  const walk = (nodes: Array<GridNodeResponse> | undefined) => {
    for (const node of nodes ?? []) {
      if (node.is_loop === true) {
        found.push(node.id);
      }
      walk(node.children ?? undefined);
    }
  };

  walk(data);

  return found;
};
