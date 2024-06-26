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

import type { NodeType, DepNode, WebserverEdge } from "src/types";
import { useQuery } from "react-query";
import useFilters from "src/dag/useFilters";

interface GenerateProps {
  nodes: DepNode[];
  edges: WebserverEdge[];
  font: string;
  openGroupIds?: string[];
  arrange: string;
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

const formatEdge = (e: WebserverEdge, font: string, node?: DepNode) => ({
  id: `${e.sourceId}-${e.targetId}`,
  sources: [e.sourceId],
  targets: [e.targetId],
  isSetupTeardown: e.isSetupTeardown,
  parentNode: node?.id,
  isSourceDataset: e.isSourceDataset,
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
});

const generateGraph = ({
  nodes,
  edges: unformattedEdges,
  font,
  openGroupIds,
  arrange,
}: GenerateProps) => {
  const closedGroupIds: string[] = [];
  let filteredEdges = unformattedEdges;

  const getNestedChildIds = (children: DepNode[]) => {
    let childIds: string[] = [];
    children.forEach((c) => {
      childIds.push(c.id);
      if (c.children) {
        const nestedChildIds = getNestedChildIds(c.children);
        childIds = [...childIds, ...nestedChildIds];
      }
    });
    return childIds;
  };

  const formatChildNode = (
    node: DepNode
  ): DepNode & {
    label: string;
    layoutOptions?: Record<string, string>;
    width?: number;
    height?: number;
  } => {
    const { id, value, children } = node;
    const isOpen = openGroupIds?.includes(id);
    const childCount =
      children?.filter((c: DepNode) => !c.id.includes("join_id")).length || 0;
    const childIds = children?.length ? getNestedChildIds(children) : [];
    if (isOpen && children?.length) {
      return {
        ...node,
        id,
        value: {
          ...value,
          childCount,
          isOpen: true,
        },
        label: value.label,
        layoutOptions: {
          "elk.padding": "[top=80,left=15,bottom=15,right=15]",
        },
        children: children.map(formatChildNode),
        edges: filteredEdges
          .filter((e) => {
            if (
              childIds.indexOf(e.sourceId) > -1 &&
              childIds.indexOf(e.targetId) > -1
            ) {
              // Remove edge from array when we add it here
              filteredEdges = filteredEdges.filter(
                (fe) =>
                  !(fe.sourceId === e.sourceId && fe.targetId === e.targetId)
              );
              return true;
            }
            return false;
          })
          .map((e) => formatEdge(e, font, node)),
      };
    }
    const isJoinNode = id.includes("join_id");
    if (!isOpen && children?.length) {
      filteredEdges = filteredEdges
        // Filter out internal group edges
        .filter(
          (e) =>
            !(
              childIds.indexOf(e.sourceId) > -1 &&
              childIds.indexOf(e.targetId) > -1
            )
        )
        // For external group edges, point to the group itself instead of a child node
        .map((e) => ({
          ...e,
          sourceId: childIds.indexOf(e.sourceId) > -1 ? node.id : e.sourceId,
          targetId: childIds.indexOf(e.targetId) > -1 ? node.id : e.targetId,
        }));
      closedGroupIds.push(id);
    }

    const label = value.isMapped ? `${value.label} [100]` : value.label;
    const labelLength = getTextWidth(label, font);
    const width = labelLength > 200 ? labelLength : 200;

    return {
      id,
      label: value.label,
      value: {
        ...value,
        isJoinNode,
        childCount,
      },
      width: isJoinNode ? 10 : width,
      height: isJoinNode ? 10 : 80,
    };
  };

  const children = nodes.map(formatChildNode);

  const edges = filteredEdges.map((e) => formatEdge(e, font));

  return {
    id: "root",
    layoutOptions: {
      hierarchyHandling: "INCLUDE_CHILDREN",
      "elk.direction": getDirection(arrange),
      "spacing.edgeLabel": "10.0",
      "elk.core.options.EdgeLabelPlacement": "CENTER",
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
}: LayoutProps) => {
  const {
    filters: { root, filterDownstream, filterUpstream },
  } = useFilters();

  return useQuery(
    [
      "graphLayout",
      nodes?.children?.length,
      openGroupIds,
      arrange,
      root,
      filterUpstream,
      filterDownstream,
    ],
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
};
