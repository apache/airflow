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
import type { GridDAGRunwithTIs, NodeResponse } from "openapi/requests/types.gen";

export type RunWithDuration = {
  duration: number;
} & GridDAGRunwithTIs;

export type GridTask = {
  depth: number;
  isGroup?: boolean;
  isOpen?: boolean;
} & NodeResponse;

export const flattenNodes = (nodes: Array<NodeResponse>, openGroupIds: Array<string>, depth: number = 0) => {
  let flatNodes: Array<GridTask> = [];
  let allGroupIds: Array<string> = [];

  nodes.forEach((node) => {
    if (node.type === "task") {
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
    }
  });

  return { allGroupIds, flatNodes };
};
