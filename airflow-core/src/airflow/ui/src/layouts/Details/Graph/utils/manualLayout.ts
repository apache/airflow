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

const collisionPadding = 12;
const searchGridSize = 24;
const maxSearchRadius = 24;
const fallbackNodeWidth = 100;
const fallbackNodeHeight = 64;
const radialDirections = [
  { x: 1, y: 0 },
  { x: -1, y: 0 },
  { x: 0, y: 1 },
  { x: 0, y: -1 },
  { x: 1, y: 1 },
  { x: 1, y: -1 },
  { x: -1, y: 1 },
  { x: -1, y: -1 },
];

const getNodeDimensions = (node: GraphNode) => ({
  height: node.height ?? node.data.height ?? fallbackNodeHeight,
  width: node.width ?? node.data.width ?? fallbackNodeWidth,
});

const isPositionNodeChange = (
  change: NodeChange<ReactFlowNode<CustomNodeProps>>,
): change is PositionNodeChange =>
  change.type === "position" && "position" in change && change.position !== undefined;

const getNodesById = (nodes: Array<GraphNode>) => new Map(nodes.map((node) => [node.id, node]));

const getAbsoluteNodePosition = ({
  node,
  nodesById,
  position = node.position,
}: {
  node: GraphNode;
  nodesById: Map<string, GraphNode>;
  position?: { x: number; y: number };
}) => {
  const absolutePosition = { ...position };
  let { parentId } = node;

  while (parentId !== undefined) {
    const parentNode = nodesById.get(parentId);

    if (parentNode === undefined) {
      return absolutePosition;
    }

    absolutePosition.x += parentNode.position.x;
    absolutePosition.y += parentNode.position.y;
    ({ parentId } = parentNode);
  }

  return absolutePosition;
};

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

const isDescendantOf = ({
  ancestorId,
  nodeId,
  nodesById,
}: {
  ancestorId: string;
  nodeId: string;
  nodesById: Map<string, GraphNode>;
}) => {
  let parentId = nodesById.get(nodeId)?.parentId;

  while (parentId !== undefined) {
    if (parentId === ancestorId) {
      return true;
    }

    parentId = nodesById.get(parentId)?.parentId;
  }

  return false;
};

const areRelatedNodes = ({
  firstNodeId,
  nodesById,
  secondNodeId,
}: {
  firstNodeId: string;
  nodesById: Map<string, GraphNode>;
  secondNodeId: string;
}) =>
  isDescendantOf({ ancestorId: firstNodeId, nodeId: secondNodeId, nodesById }) ||
  isDescendantOf({ ancestorId: secondNodeId, nodeId: firstNodeId, nodesById });

const shiftNodes = ({
  delta,
  nodeIds,
  nodes,
}: {
  delta: { x: number; y: number };
  nodeIds: Set<string>;
  nodes: Array<GraphNode>;
}) =>
  nodes.map((node) =>
    nodeIds.has(node.id)
      ? { ...node, position: { x: node.position.x + delta.x, y: node.position.y + delta.y } }
      : node,
  );

const nodesOverlap = ({
  firstNode,
  firstPosition = firstNode.position,
  nodesById,
  secondNode,
}: {
  firstNode: GraphNode;
  firstPosition?: { x: number; y: number };
  nodesById: Map<string, GraphNode>;
  secondNode: GraphNode;
}) => {
  const firstDimensions = getNodeDimensions(firstNode);
  const secondDimensions = getNodeDimensions(secondNode);
  const firstAbsolutePosition = getAbsoluteNodePosition({
    node: firstNode,
    nodesById,
    position: firstPosition,
  });
  const secondAbsolutePosition = getAbsoluteNodePosition({ node: secondNode, nodesById });

  return (
    firstAbsolutePosition.x < secondAbsolutePosition.x + secondDimensions.width + collisionPadding &&
    firstAbsolutePosition.x + firstDimensions.width + collisionPadding > secondAbsolutePosition.x &&
    firstAbsolutePosition.y < secondAbsolutePosition.y + secondDimensions.height + collisionPadding &&
    firstAbsolutePosition.y + firstDimensions.height + collisionPadding > secondAbsolutePosition.y
  );
};

const findNearestOpenPosition = ({
  movedNode,
  movingNodeIds,
  nodes,
}: {
  movedNode: GraphNode;
  movingNodeIds: Set<string>;
  nodes: Array<GraphNode>;
}) => {
  const nodesById = getNodesById(nodes);
  const movingNodes = nodes.filter((node) => movingNodeIds.has(node.id));
  const otherNodes = nodes.filter(
    (node) =>
      !movingNodeIds.has(node.id) &&
      movingNodes.every(
        (movingNode) => !areRelatedNodes({ firstNodeId: movingNode.id, nodesById, secondNodeId: node.id }),
      ),
  );

  const isPositionFree = (position: { x: number; y: number }) => {
    const delta = {
      x: position.x - movedNode.position.x,
      y: position.y - movedNode.position.y,
    };

    return movingNodes.every((movingNode) => {
      const shiftedPosition = {
        x: movingNode.position.x + delta.x,
        y: movingNode.position.y + delta.y,
      };

      return otherNodes.every(
        (node) =>
          !nodesOverlap({
            firstNode: movingNode,
            firstPosition: shiftedPosition,
            nodesById,
            secondNode: node,
          }),
      );
    });
  };

  if (isPositionFree(movedNode.position)) {
    return movedNode.position;
  }

  for (let radius = 1; radius <= maxSearchRadius; radius += 1) {
    for (const direction of radialDirections) {
      const position = {
        x: movedNode.position.x + direction.x * radius * searchGridSize,
        y: movedNode.position.y + direction.y * radius * searchGridSize,
      };

      if (isPositionFree(position)) {
        return position;
      }
    }
  }

  return movedNode.position;
};

const avoidNodeOverlap = ({
  changes,
  nodes,
}: {
  changes: Array<NodeChange<ReactFlowNode<CustomNodeProps>>>;
  nodes: Array<GraphNode>;
}) => {
  const movedNodeIds = new Set(
    changes
      .filter(
        (
          change,
        ): change is {
          dragging: false;
          id: string;
          type: "position";
        } & NodeChange<ReactFlowNode<CustomNodeProps>> =>
          change.type === "position" && change.dragging === false,
      )
      .map((change) => change.id),
  );

  if (movedNodeIds.size === 0) {
    return nodes;
  }

  let resolvedNodes = [...nodes];

  for (const movedNodeId of movedNodeIds) {
    const movedNodeIndex = resolvedNodes.findIndex((node) => node.id === movedNodeId);
    const movedNode = movedNodeIndex === -1 ? undefined : resolvedNodes[movedNodeIndex];

    if (movedNode !== undefined) {
      const movingNodeIds = new Set([movedNode.id]);
      const position = findNearestOpenPosition({ movedNode, movingNodeIds, nodes: resolvedNodes });

      if (position !== movedNode.position) {
        const delta = {
          x: position.x - movedNode.position.x,
          y: position.y - movedNode.position.y,
        };

        resolvedNodes = shiftNodes({ delta, nodeIds: movingNodeIds, nodes: resolvedNodes });
      }
    }
  }

  return resolvedNodes;
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

export const applyManualNodeChanges = ({
  changes,
  currentNodes,
}: {
  changes: Array<NodeChange<ReactFlowNode<CustomNodeProps>>>;
  currentNodes: Array<GraphNode>;
}) => {
  const changedNodes = applyNodeChanges(changes, currentNodes);

  return avoidNodeOverlap({ changes, nodes: changedNodes });
};
