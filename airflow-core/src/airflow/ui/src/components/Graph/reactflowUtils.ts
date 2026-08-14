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
import type { Node as FlowNodeType, Edge as FlowEdgeType } from "@xyflow/react";
import type { ElkExtendedEdge } from "elkjs";

import type { LightGridTaskInstanceSummary, NodeResponse } from "openapi/requests/types.gen";

import type { LayoutNode } from "./useGraphLayout";

export type CustomNodeProps = {
  assetCondition?: NodeResponse["asset_condition_type"];
  childCount?: number;
  depth?: number;
  height?: number;
  id: string;
  isFiltered?: boolean;
  isGroup?: boolean;
  isMapped?: boolean;
  isOpen?: boolean;
  isSelected?: boolean;
  label: string;
  operator?: string | null;
  setupTeardownType?: NodeResponse["setup_teardown_type"];
  taskInstance?: LightGridTaskInstanceSummary;
  team?: string | null;
  tooltip?: string | null;
  type: string;
  uiColor?: string | null;
  uiFgcolor?: string | null;
  width?: number;
};

type NodeType = FlowNodeType<CustomNodeProps>;

type FlattenNodesProps = {
  children?: Array<LayoutNode>;
  level?: number;
  parent?: NodeType;
};

// Generate a flattened list of nodes for react-flow to render.
// Uses push() throughout to avoid the O(n²) spread-in-loop pattern that the
// previous implementation had (edges = [...edges, ...newEdges] per node).
export const flattenGraph = ({
  children,
  level = 0,
  parent,
}: FlattenNodesProps): {
  edges: Array<ElkExtendedEdge>;
  nodes: Array<NodeType>;
} => {
  const nodes: Array<NodeType> = [];
  const edges: Array<ElkExtendedEdge> = [];

  if (!children) {
    return { edges, nodes };
  }
  const parentNode = parent ? { parentNode: parent.id } : undefined;

  children.forEach((node) => {
    const x = (parent?.position.x ?? 0) + (node.x ?? 0);
    const y = (parent?.position.y ?? 0) + (node.y ?? 0);
    const newNode = {
      data: { ...node, depth: level },
      height: node.height,
      id: node.id,
      position: { x, y },
      type: node.type,
      width: node.width,
      ...parentNode,
    } satisfies NodeType;

    nodes.push(newNode);

    for (const edge of node.edges ?? []) {
      edges.push({
        ...edge,
        labels: edge.labels?.map((label) => ({
          ...label,
          x: (label.x ?? 0) + x,
          y: (label.y ?? 0) + y,
        })),
        sections: edge.sections?.map((section) => ({
          ...section,
          bendPoints: section.bendPoints?.map((bp) => ({ x: bp.x + x, y: bp.y + y })),
          endPoint: { x: section.endPoint.x + x, y: section.endPoint.y + y },
          startPoint: { x: section.startPoint.x + x, y: section.startPoint.y + y },
        })),
      });
    }

    if (node.children) {
      const { edges: childEdges, nodes: childNodes } = flattenGraph({
        children: node.children as Array<LayoutNode>,
        level: level + 1,
        parent: newNode,
      });

      nodes.push(...childNodes);
      edges.push(...childEdges);
    }
  });

  return { edges, nodes };
};

type Edge = {
  parentNode?: string;
} & ElkExtendedEdge;

export type EdgeData = {
  rest: {
    edgeType?: "data" | "scheduling";
    isFiltered?: boolean;
    isSelected?: boolean;
    isSetupTeardown?: boolean;
  } & ElkExtendedEdge;
};

export const formatFlowEdges = ({ edges }: { edges: Array<Edge> }): Array<FlowEdgeType<EdgeData>> =>
  edges.map((edge) => ({
    data: { rest: edge },
    id: edge.id,
    source: edge.sources[0] ?? "",
    target: edge.targets[0] ?? "",
    type: "custom",
  }));

type SelectionGraphEdge = { id: string; source: string; target: string };
type SelectionGraphNode = { id: string; type?: string };

/**
 * Edges only highlight when one of their two endpoints has node-level `isSelected` set, or (for
 * gate-adjacent edges) when this function marks that specific edge id. Gate edges are directional
 * -- `source` is always the thing feeding the condition, `target` is what it schedules -- so:
 *
 * - Entering a gate from one of its inputs (an asset, or a nested gate below it) continues
 *   through the gate's single output edge only. It does NOT fan out to the gate's other inputs,
 *   which is exactly the over-highlighting bug this replaces: selecting one asset in an AND/OR
 *   condition must not also light up sibling assets that happen to share the same gate.
 * - Entering a gate from its output (e.g. selecting the Dag it schedules) fans out to every one
 *   of its inputs, since all of them are genuinely part of what makes that Dag's condition true.
 *
 * Returns the set of edge ids to mark selected -- callers OR this into their own per-edge
 * `isSelected` computation (see Graph.tsx / AssetGraph.tsx).
 */
export const getGatePathEdgeIdsForSelection = (
  nodes: Array<SelectionGraphNode>,
  edges: Array<SelectionGraphEdge>,
  isNodeSelected: (nodeId: string) => boolean,
): Set<string> => {
  const gateNodeIds = new Set(nodes.filter((node) => node.type === "asset-condition").map((node) => node.id));

  if (gateNodeIds.size === 0) {
    return new Set();
  }

  const outgoingByNode = new Map<string, Array<SelectionGraphEdge>>();
  const incomingByNode = new Map<string, Array<SelectionGraphEdge>>();
  const addTo = (map: Map<string, Array<SelectionGraphEdge>>, key: string, edge: SelectionGraphEdge) => {
    const existing = map.get(key);

    if (existing) {
      existing.push(edge);
    } else {
      map.set(key, [edge]);
    }
  };

  edges.forEach((edge) => {
    addTo(outgoingByNode, edge.source, edge);
    addTo(incomingByNode, edge.target, edge);
  });

  const selectedEdgeIds = new Set<string>();
  const visitedForward = new Set<string>();
  const visitedBackward = new Set<string>();

  const walkForward = (gateId: string) => {
    if (visitedForward.has(gateId)) {
      return;
    }
    visitedForward.add(gateId);

    (outgoingByNode.get(gateId) ?? []).forEach((edge) => {
      selectedEdgeIds.add(edge.id);

      if (gateNodeIds.has(edge.target)) {
        walkForward(edge.target);
      }
    });
  };

  const walkBackward = (gateId: string) => {
    if (visitedBackward.has(gateId)) {
      return;
    }
    visitedBackward.add(gateId);

    (incomingByNode.get(gateId) ?? []).forEach((edge) => {
      selectedEdgeIds.add(edge.id);

      if (gateNodeIds.has(edge.source)) {
        walkBackward(edge.source);
      }
    });
  };

  nodes.forEach((node) => {
    if (!isNodeSelected(node.id)) {
      return;
    }

    (outgoingByNode.get(node.id) ?? []).forEach((edge) => {
      selectedEdgeIds.add(edge.id);

      if (gateNodeIds.has(edge.target)) {
        walkForward(edge.target);
      }
    });

    (incomingByNode.get(node.id) ?? []).forEach((edge) => {
      selectedEdgeIds.add(edge.id);

      if (gateNodeIds.has(edge.source)) {
        walkBackward(edge.source);
      }
    });
  });

  return selectedEdgeIds;
};
