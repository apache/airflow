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
import { applyNodeChanges, type Node as ReactFlowNode, type NodeChange } from "@xyflow/react";

import type { CustomNodeProps } from "src/components/Graph/reactflowUtils";

export type GraphNode = ReactFlowNode<CustomNodeProps>;

type PositionNodeChange = {
  dragging?: boolean;
  id: string;
  position: { x: number; y: number };
  type: "position";
} & NodeChange<ReactFlowNode<CustomNodeProps>>;

const isPositionNodeChange = (
  change: NodeChange<ReactFlowNode<CustomNodeProps>>,
): change is PositionNodeChange =>
  change.type === "position" && "position" in change && change.position !== undefined;

const getChildIdsByParentId = (nodes: Array<GraphNode>) => {
  const childIdsByParentId = new Map<string, Array<string>>();

  for (const node of nodes) {
    if (node.parentId !== undefined) {
      const childIds = childIdsByParentId.get(node.parentId) ?? [];

      childIds.push(node.id);
      childIdsByParentId.set(node.parentId, childIds);
    }
  }

  return childIdsByParentId;
};

const getDescendantIds = (nodes: Array<GraphNode>, nodeId: string) => {
  const childIdsByParentId = getChildIdsByParentId(nodes);
  const descendantIds = new Set<string>();
  const pendingIds = [...(childIdsByParentId.get(nodeId) ?? [])];

  while (pendingIds.length > 0) {
    const pendingId = pendingIds.pop();

    if (pendingId !== undefined && !descendantIds.has(pendingId)) {
      descendantIds.add(pendingId);
      pendingIds.push(...(childIdsByParentId.get(pendingId) ?? []));
    }
  }

  return descendantIds;
};

export const getManualEdgeNodeIds = ({
  changes,
  nodes,
}: {
  changes: Array<NodeChange<ReactFlowNode<CustomNodeProps>>>;
  nodes: Array<GraphNode>;
}) => {
  const nodeIds = new Set<string>();

  for (const change of changes.filter(isPositionNodeChange)) {
    nodeIds.add(change.id);

    for (const descendantId of getDescendantIds(nodes, change.id)) {
      nodeIds.add(descendantId);
    }
  }

  return nodeIds;
};

// Rearrange mode is intentionally temporary: users can overlap nodes while
// exploring a layout, and leaving the mode restores ELK positions.
export const applyManualNodeChanges = ({
  changes,
  currentNodes,
}: {
  changes: Array<NodeChange<ReactFlowNode<CustomNodeProps>>>;
  currentNodes: Array<GraphNode>;
}) => applyNodeChanges(changes, currentNodes);
