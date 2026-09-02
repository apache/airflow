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
import innerText from "react-innertext";

import type { ParsedLogEntry } from "src/queries/useLogs";

import { getGroupHeaderMarker } from "./utils";

type RowRange = {
  end: number;
  start: number;
};

/**
 * Map a DOM node inside the virtualized log list to the `data-index` of the
 * row containing it.
 */
export const getRowIndexForNode = (node: Node | null, container: HTMLElement): number | undefined => {
  const element = node instanceof Element ? node : node?.parentElement;
  const row = element?.closest("[data-index]");

  if (!row || !container.contains(row)) {
    return undefined;
  }
  const index = Number(row.getAttribute("data-index"));

  return Number.isInteger(index) ? index : undefined;
};

/**
 * Row-index range covered by the current text selection, if both selection
 * boundaries sit inside log rows of `container`.
 */
export const getSelectionRowRange = (
  selection: Selection | null,
  container: HTMLElement,
): RowRange | undefined => {
  if (!selection || selection.isCollapsed || selection.rangeCount === 0) {
    return undefined;
  }
  const range = selection.getRangeAt(0);
  const start = getRowIndexForNode(range.startContainer, container);
  const end = getRowIndexForNode(range.endContainer, container);

  if (start === undefined || end === undefined) {
    return undefined;
  }

  return start <= end ? { end, start } : { end: start, start: end };
};

/**
 * Row indexes to pin so the virtualizer keeps selection-boundary rows
 * mounted. Boundaries map independently (the drag focus may sit off the
 * rows) and a collapsed caret pins too, for shift-click extension.
 */
export const getSelectionPinnedRows = (
  selection: Selection | null,
  container: HTMLElement,
): Array<number> => {
  if (!selection || selection.rangeCount === 0) {
    return [];
  }
  const range = selection.getRangeAt(0);

  return [
    getRowIndexForNode(range.startContainer, container),
    getRowIndexForNode(range.endContainer, container),
  ].filter((index): index is number => index !== undefined);
};

type BottomDragClampOptions = {
  container: HTMLElement;
  pointerY: number;
  selection: Selection;
};

type BottomDragBoundary = {
  lastRow: Element;
  y: number;
};

/**
 * The first Y coordinate where a downward drag leaves selectable log text.
 * Mid-scroll this is the scrollport edge; at the end it is the last mounted
 * row edge, before any trailing space or container padding.
 */
export const getBottomDragBoundary = (container: HTMLElement): BottomDragBoundary | undefined => {
  const rows = container.querySelectorAll("[data-index]");
  const lastRow = rows[rows.length - 1];

  if (lastRow === undefined) {
    return undefined;
  }

  return {
    lastRow,
    y: Math.min(container.getBoundingClientRect().bottom, lastRow.getBoundingClientRect().bottom),
  };
};

/**
 * Re-extend a downward drag selection to the last mounted row. When all log
 * rows are absolutely positioned, Chrome can resolve a hit-test below the
 * scrollport's text to the block start, reversing a downward selection.
 */
export const getBottomDragClampTarget = ({
  container,
  pointerY,
  selection,
}: BottomDragClampOptions): { node: Node; offset: number } | undefined => {
  if (selection.rangeCount === 0) {
    return undefined;
  }
  const anchorRow = getRowIndexForNode(selection.anchorNode, container);

  if (anchorRow === undefined) {
    return undefined;
  }
  const boundary = getBottomDragBoundary(container);

  if (boundary === undefined || pointerY < boundary.y) {
    return undefined;
  }
  const { lastRow } = boundary;
  const offset = lastRow.childNodes.length;

  if (selection.focusNode === lastRow && selection.focusOffset === offset) {
    return undefined;
  }

  return { node: lastRow, offset };
};

/**
 * Merge selection-pinned row indexes into the virtualizer's default render
 * range. Rows holding selection boundaries must stay mounted while the user
 * scrolls — unmounting a boundary node collapses the browser selection.
 */
export const mergePinnedIndexes = (
  defaultIndexes: Array<number>,
  pinnedIndexes: Array<number>,
  count: number,
): Array<number> => {
  const validPins = pinnedIndexes.filter((index) => index >= 0 && index < count);

  if (validPins.length === 0) {
    return defaultIndexes;
  }

  return [...new Set([...validPins, ...defaultIndexes])].sort((first, second) => first - second);
};

/**
 * Canonical plain text of a parsed log entry for clipboard rebuilding:
 * group headers rebuild as the on-screen `▶/▼ name` form (marker follows
 * the group's current expand state), log lines render through the
 * plain-text pipeline, and the innerText fallback covers synthetic
 * entries such as the TI-context preamble.
 */
export const getEntryText = (entry: ParsedLogEntry, expandedGroupIds?: ReadonlySet<number>): string => {
  if (typeof entry.element === "string") {
    return entry.group?.type === "header"
      ? `${getGroupHeaderMarker(expandedGroupIds?.has(entry.group.id) ?? false)} ${entry.element}`
      : entry.element;
  }
  if (entry.getPlainText) {
    return entry.getPlainText();
  }

  return entry.element ? innerText(entry.element) : "";
};

/**
 * Range text with copy-excluded elements (the line-number links) removed.
 * Native copy drops them via `user-select: none`, but programmatic
 * `Range.toString()` ignores CSS, so strip them explicitly.
 */
const getRangeText = (range: Range): string => {
  const fragment = range.cloneContents();

  for (const element of fragment.querySelectorAll("[data-copy-exclude]")) {
    element.remove();
  }

  return fragment.textContent;
};

type ExtractSelectedLogTextOptions = {
  container: HTMLElement;
  getRowText: (index: number) => string;
  selection: Selection;
};

/**
 * Rebuild the text of a multi-row selection from log data. Native copy
 * serializes the DOM, so it silently drops selected rows the virtualizer has
 * unmounted. Returns undefined when native copy is already exact (single row
 * or every selected row mounted) or when the selection cannot be mapped to
 * log rows.
 */
export const extractSelectedLogText = ({
  container,
  getRowText,
  selection,
}: ExtractSelectedLogTextOptions): string | undefined => {
  // Firefox multi-range selections: rebuilding only range 0 would clobber the rest.
  if (selection.rangeCount !== 1) {
    return undefined;
  }
  const rowRange = getSelectionRowRange(selection, container);

  if (!rowRange || rowRange.start === rowRange.end) {
    return undefined;
  }

  const mountedIndexes = new Set(
    [...container.querySelectorAll("[data-index]")].map((row) => Number(row.getAttribute("data-index"))),
  );

  let hasUnmountedRow = false;

  for (let index = rowRange.start + 1; index < rowRange.end; index += 1) {
    if (!mountedIndexes.has(index)) {
      hasUnmountedRow = true;
      break;
    }
  }

  if (!hasUnmountedRow) {
    return undefined;
  }

  const firstRow = container.querySelector(`[data-index="${rowRange.start}"]`);
  const lastRow = container.querySelector(`[data-index="${rowRange.end}"]`);

  if (!firstRow || !lastRow) {
    return undefined;
  }

  const range = selection.getRangeAt(0);
  const firstPartial = document.createRange();

  firstPartial.selectNodeContents(firstRow);
  firstPartial.setStart(range.startContainer, range.startOffset);

  const lastPartial = document.createRange();

  lastPartial.selectNodeContents(lastRow);
  lastPartial.setEnd(range.endContainer, range.endOffset);

  const lines = [getRangeText(firstPartial)];

  for (let index = rowRange.start + 1; index < rowRange.end; index += 1) {
    lines.push(getRowText(index));
  }
  lines.push(getRangeText(lastPartial));

  return lines.join("\n");
};
