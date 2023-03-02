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

import ELK, { ElkExtendedEdge, ElkShape } from "elkjs";

import type { DepNode } from "src/types";
import type { NodeType } from "src/datasets/Graph/Node";
import { useQuery } from "react-query";

interface GenerateProps {
  nodes: DepNode[];
  edges: WebserverEdge[];
  font: string;
  openGroupIds?: string[];
  arrange: string;
}

interface WebserverEdge {
  label?: string;
  sourceId: string;
  targetId: string;
}

interface Graph extends ElkShape {
  children: NodeType[];
  edges: ElkExtendedEdge[];
}

interface LayoutProps {
  edges?: WebserverEdge[];
  nodes?: DepNode;
  openGroupIds?: string[];
  arrange?: string;
}

// Take text and font to calculate how long each node should be
export function getTextWidth(text: string, font: string) {
  const context = document.createElement("canvas").getContext("2d");
  if (context) {
    context.font = font;
    const metrics = context.measureText(text);
    return metrics.width;
  }
  return text.length * 9;
}

const getDirection = (arrange: string) => {
  switch (arrange) {
    case "RL":
      return "LEFT";
    case "TB":
      return "DOWN";
    case "BT":
      return "UP";
    case "LR":
    default:
      return "RIGHT";
  }
};

const generateGraph = ({
  nodes,
  edges: unformattedEdges,
  font,
  openGroupIds,
  arrange,
}: GenerateProps) => {
  const closedGroupIds: string[] = [];

  const formatChildNode = (node: any) => {
    const { id, value, children } = node;
    const isOpen = openGroupIds?.includes(value.label);
    if (isOpen && children.length) {
      return {
        id,
        value: {
          ...value,
          childCount: children.length,
          isOpen: true,
        },
        label: value.label,
        layoutOptions: {
          "elk.padding": "[top=60,left=10,bottom=10,right=10]",
        },
        children: children.map(formatChildNode),
      };
    }
    const isJoinNode = id.includes("join_id");
    if (children?.length) closedGroupIds.push(value.label);
    return {
      id,
      label: value.label,
      value: {
        ...value,
        isJoinNode,
        childCount: children?.length || 0,
      },
      width: isJoinNode ? 10 : 200,
      height: isJoinNode ? 10 : 60,
    };
  };
  const children = nodes.map(formatChildNode);

  const edges = unformattedEdges
    .map((edge) => {
      let { sourceId, targetId } = edge;
      const splitSource = sourceId.split(".");
      const splitTarget = targetId.split(".");

      if (closedGroupIds.includes(splitSource[splitSource.length - 2])) {
        splitSource.pop();
        sourceId = splitSource.join(".");
      }
      if (closedGroupIds.includes(splitTarget[splitTarget.length - 2])) {
        splitTarget.pop();
        targetId = splitTarget.join(".");
      }
      return {
        ...edge,
        targetId,
        sourceId,
      };
    })
    // Deduplicate edges
    .filter(
      (value, index, self) =>
        index ===
        self.findIndex(
          (t) => t.sourceId === value.sourceId && t.targetId === value.targetId
        )
    )
    .filter((edge) => {
      const splitSource = edge.sourceId.split(".");
      const splitTarget = edge.targetId.split(".");
      if (
        splitSource
          .slice(0, splitSource.length - 1)
          .some((id) => closedGroupIds.includes(id)) ||
        splitTarget
          .slice(0, splitTarget.length - 1)
          .some((id) => closedGroupIds.includes(id))
      ) {
        return false;
      }
      if (edge.sourceId === edge.targetId) return false;
      return true;
    })
    .map((e) => ({
      id: `${e.sourceId}-${e.targetId}`,
      sources: [e.sourceId],
      targets: [e.targetId],
      labels: e.label
        ? [
            {
              id: e.label,
              text: e.label,
              height: 16,
              width: getTextWidth(e.label, font),
            },
          ]
        : [],
    }));

  return {
    id: "root",
    layoutOptions: {
      hierarchyHandling: "INCLUDE_CHILDREN",
      "elk.direction": getDirection(arrange),
      "spacing.edgeLabel": "10.0",
    },
    children,
    edges,
  };
};

export const useGraphLayout = ({
  edges = [],
  nodes,
  openGroupIds,
  arrange = "LR",
}: LayoutProps) =>
  useQuery(
    ["graphLayout", !!nodes?.children, openGroupIds, arrange],
    async () => {
      const font = `bold ${16}px ${
        window.getComputedStyle(document.body).fontFamily
      }`;
      const elk = new ELK();
      const data = await elk.layout(
        generateGraph({
          nodes: nodes?.children || [],
          edges,
          font,
          openGroupIds,
          arrange,
        })
      );
      return data as Graph;
    }
  );
