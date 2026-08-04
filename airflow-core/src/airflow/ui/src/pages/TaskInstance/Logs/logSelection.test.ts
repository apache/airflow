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
import { afterEach, describe, expect, it } from "vitest";

import { getSelectionPinnedRows, mergePinnedIndexes } from "./logSelection";

const buildLogContainer = (rows: Array<{ index: number; text: string }>): HTMLElement => {
  const container = document.createElement("div");

  rows.forEach(({ index, text }) => {
    const row = document.createElement("div");

    row.setAttribute("data-index", String(index));
    row.textContent = text;
    container.append(row);
  });
  document.body.append(container);

  return container;
};

const getRowTextNode = (container: HTMLElement, index: number): Node =>
  container.querySelector(`[data-index="${index}"]`)?.firstChild as Node;

const makeSelection = (range: Range): Selection =>
  ({
    getRangeAt: () => range,
    isCollapsed: range.collapsed,
    rangeCount: 1,
  }) as unknown as Selection;

type SelectBetweenOptions = {
  end: Node;
  endOffset: number;
  start: Node;
  startOffset: number;
};

const selectBetween = ({ end, endOffset, start, startOffset }: SelectBetweenOptions): Selection => {
  const range = document.createRange();

  range.setStart(start, startOffset);
  range.setEnd(end, endOffset);

  return makeSelection(range);
};

afterEach(() => {
  document.body.innerHTML = "";
});

describe("getSelectionPinnedRows", () => {
  it("pins both rows when both boundaries are inside log rows", () => {
    const container = buildLogContainer([
      { index: 2, text: "line 2" },
      { index: 7, text: "line 7" },
    ]);
    const selection = selectBetween({
      end: getRowTextNode(container, 7),
      endOffset: 3,
      start: getRowTextNode(container, 2),
      startOffset: 1,
    });

    expect(getSelectionPinnedRows(selection, container)).toEqual([2, 7]);
  });

  it("keeps the anchor row pinned when the drag focus leaves the rows", () => {
    const toolbar = document.createElement("div");

    toolbar.textContent = "search toolbar";
    document.body.prepend(toolbar);

    const container = buildLogContainer([{ index: 100, text: "anchor line" }]);

    const selection = selectBetween({
      end: getRowTextNode(container, 100),
      endOffset: 5,
      start: toolbar.firstChild as Node,
      startOffset: 0,
    });

    expect(getSelectionPinnedRows(selection, container)).toEqual([100]);
  });

  it("pins only the mapped row when one boundary sits on the container padding", () => {
    const container = buildLogContainer([
      { index: 0, text: "line 0" },
      { index: 5, text: "line 5" },
    ]);
    const range = document.createRange();

    range.setStart(container, 0);
    range.setEnd(getRowTextNode(container, 5), 3);

    expect(getSelectionPinnedRows(makeSelection(range), container)).toEqual([5]);
  });

  it("pins the caret row for a collapsed selection so shift-click extension survives scrolling", () => {
    const container = buildLogContainer([{ index: 3, text: "caret line" }]);
    const node = getRowTextNode(container, 3);

    expect(
      getSelectionPinnedRows(
        selectBetween({ end: node, endOffset: 2, start: node, startOffset: 2 }),
        container,
      ),
    ).toEqual([3, 3]);
  });

  it("returns no pins for a null selection", () => {
    const container = buildLogContainer([{ index: 0, text: "line 0" }]);

    expect(getSelectionPinnedRows(null, container)).toEqual([]);
  });
});

describe("mergePinnedIndexes", () => {
  it("returns the default range untouched when there is nothing to pin", () => {
    expect(mergePinnedIndexes([5, 6, 7], [], 10)).toEqual([5, 6, 7]);
  });

  it("merges pinned indexes into the range, sorted and deduplicated", () => {
    expect(mergePinnedIndexes([5, 6, 7], [12, 2, 6], 20)).toEqual([2, 5, 6, 7, 12]);
  });

  it("drops pinned indexes outside [0, count)", () => {
    expect(mergePinnedIndexes([5, 6], [-1, 99], 10)).toEqual([5, 6]);
    expect(mergePinnedIndexes([5, 6], [-1, 2, 99], 10)).toEqual([2, 5, 6]);
  });
});
