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
  isGroup?: boolean;
  isMapped?: boolean;
  isOpen?: boolean;
  isSelected?: boolean;
  label: string;
  operator?: string | null;
  setupTeardownType?: NodeResponse["setup_teardown_type"];
  taskInstance?: LightGridTaskInstanceSummary;
  type: string;
  width?: number;
};

type NodeType = FlowNodeType<CustomNodeProps>;

type FlattenNodesProps = {
  children?: Array<LayoutNode>;
  level?: number;
  parent?: NodeType;
};

// Generate a flattened list of nodes for react-flow to render
export const flattenGraph = ({
  children,
  level = 0,
  parent,
}: FlattenNodesProps): {
  edges: Array<ElkExtendedEdge>;
  nodes: Array<NodeType>;
} => {
  let nodes: Array<NodeType> = [];
  let edges: Array<ElkExtendedEdge> = [];

  if (!children) {
    return { edges, nodes };
  }
  const parentNode = parent ? { parentNode: parent.id } : undefined;

  children.forEach((node) => {
    const x = (parent?.position.x ?? 0) + (node.x ?? 0);
    const y = (parent?.position.y ?? 0) + (node.y ?? 0);
    const newNode = {
      data: { ...node, depth: level },
      id: node.id,
      position: {
        x,
        y,
      },
      type: node.type,
      ...parentNode,
    } satisfies NodeType;

    edges = [
      ...edges,
      ...(node.edges ?? []).map((edge) => ({
        ...edge,
        labels: edge.labels?.map((label) => ({
          ...label,
          x: (label.x ?? 0) + x,
          y: (label.y ?? 0) + y,
        })),
        sections: edge.sections?.map((section) => ({
          ...section,
          // eslint-disable-next-line max-nested-callbacks
          bendPoints: section.bendPoints?.map((bp) => ({
            x: bp.x + x,
            y: bp.y + y,
          })),
          endPoint: {
            x: section.endPoint.x + x,
            y: section.endPoint.y + y,
          },
          startPoint: {
            x: section.startPoint.x + x,
            y: section.startPoint.y + y,
          },
        })),
      })),
    ];

    nodes.push(newNode);

    if (node.children) {
      const { edges: childEdges, nodes: childNodes } = flattenGraph({
        children: node.children as Array<LayoutNode>,
        level: level + 1,
        parent: newNode,
      });

      nodes = [...nodes, ...childNodes];
      edges = [...edges, ...childEdges];
    }
  });

  return {
    edges,
    nodes,
  };
};

type Edge = {
  parentNode?: string;
} & ElkExtendedEdge;

export type EdgeData = {
  rest: { isSelected?: boolean; isSetupTeardown?: boolean } & ElkExtendedEdge;
};

export const formatFlowEdges = ({ edges }: { edges: Array<Edge> }): Array<FlowEdgeType<EdgeData>> =>
  edges.map((edge) => ({
    data: { rest: edge },
    id: edge.id,
    source: edge.sources[0] ?? "",
    target: edge.targets[0] ?? "",
    type: "custom",
  }));
