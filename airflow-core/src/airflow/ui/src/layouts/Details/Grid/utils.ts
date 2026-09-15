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
import type { GridNodeResponse, NodeResponse } from "openapi/requests/types.gen";

import { ROW_HEIGHT } from "./constants";

export type GridTask = {
  depth: number;
  isGroup?: boolean;
  isOpen?: boolean;
} & GridNodeResponse;

/** Minimum width for the task-name column (matches prior `minWidth="200px"`). */
const TASK_NAME_COLUMN_MIN_WIDTH_PX = 200;

/**
 * Chakra `rem` is typically 16px. Must match TaskNames `indent(depth)`:
 * `(depth * 0.75 + 0.5)rem`.
 */
const ROOT_FONT_SIZE_PX = 16;

const indentRem = (depth: number) => depth * 0.75 + 0.5;

/**
 * Approximate rendered width for the task-name column when the Gantt is shown.
 * Task rows use absolute positioning, so the parent needs an explicit width.
 */
export const estimateTaskNameColumnWidthPx = (nodes: Array<GridTask>): number => {
  let max = TASK_NAME_COLUMN_MIN_WIDTH_PX;

  for (const node of nodes) {
    const indentPx = indentRem(node.depth) * ROOT_FONT_SIZE_PX;
    // TaskNames uses fontSize="sm" (~14px); average glyph width ~8px for mixed labels.
    const labelChars =
      node.label.length +
      (Boolean(node.is_mapped) ? 6 : 0) +
      (node.setup_teardown_type === "setup" || node.setup_teardown_type === "teardown" ? 4 : 0);
    const textPx = labelChars * 8;
    const groupChevronPx = node.isGroup ? 28 : 0;
    const paddingPx = 16;

    max = Math.max(max, Math.ceil(indentPx + textPx + groupChevronPx + paddingPx));
  }

  return max;
};

export const flattenNodes = (
  nodes: Array<GridNodeResponse> | undefined,
  openGroupIds: Array<string>,
  depth: number = 0,
) => {
  let flatNodes: Array<GridTask> = [];
  let allGroupIds: Array<string> = [];

  nodes?.forEach((node) => {
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
  });

  return { allGroupIds, flatNodes };
};

export const flattenGraphNodes = (
  nodes: Array<NodeResponse>,
): { allGroupIds: Array<string>; allOperators: Array<string> } => {
  let allGroupIds: Array<string> = [];
  const operators = new Set<string>();

  nodes.forEach((node) => {
    if (node.operator !== undefined && node.operator !== null) {
      operators.add(node.operator);
    }
    if (node.children) {
      allGroupIds.push(node.id);

      const { allGroupIds: childGroupIds, allOperators: childOperators } = flattenGraphNodes(node.children);

      allGroupIds = [...allGroupIds, ...childGroupIds];
      childOperators.forEach((op) => operators.add(op));
    }
  });

  return { allGroupIds, allOperators: [...operators].sort() };
};

// Start scrolling before the row touches an edge, so it never sits right at the border.
const SCROLL_EDGE_MARGIN_PX = ROW_HEIGHT * 2;

type ScrollDecision = {
  clientHeight: number;
  flatNodes: Array<GridTask>;
  headerPad: number;
  scrollTop: number;
  selectedGroupId?: string;
  selectedTaskId?: string;
};

/** Row index that should be centered, or undefined when the current scroll is fine. */
export const getRowIndexToScrollTo = ({
  clientHeight,
  flatNodes,
  headerPad,
  scrollTop,
  selectedGroupId,
  selectedTaskId,
}: ScrollDecision): number | undefined => {
  const anchorId = selectedTaskId ?? selectedGroupId;

  if (anchorId === undefined || flatNodes.length === 0) {
    return undefined;
  }
  const index = flatNodes.findIndex((node) => node.id === anchorId);

  if (index === -1) {
    return undefined;
  }
  if (clientHeight > 0) {
    const rowTop = headerPad + index * ROW_HEIGHT;
    const rowBottom = rowTop + ROW_HEIGHT;
    const viewTop = scrollTop + headerPad;
    const viewBottom = scrollTop + clientHeight;

    if (rowTop >= viewTop + SCROLL_EDGE_MARGIN_PX && rowBottom <= viewBottom - SCROLL_EDGE_MARGIN_PX) {
      return undefined;
    }
  }

  return index;
};
