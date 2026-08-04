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
