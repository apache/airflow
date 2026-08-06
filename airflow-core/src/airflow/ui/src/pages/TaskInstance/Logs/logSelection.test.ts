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

import { getBottomDragClampTarget, getSelectionPinnedRows, mergePinnedIndexes } from "./logSelection";

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

const makeDirectionalSelection = (options: {
  anchor: Node;
  anchorOffset: number;
  focus: Node;
  focusOffset: number;
}): Selection =>
  ({
    anchorNode: options.anchor,
    anchorOffset: options.anchorOffset,
    focusNode: options.focus,
    focusOffset: options.focusOffset,
    isCollapsed: false,
    rangeCount: 1,
  }) as unknown as Selection;

const buildClampContainer = ({
  containerBottom = 500,
  lastRowBottom = 700,
}: { containerBottom?: number; lastRowBottom?: number } = {}) => {
  const container = buildLogContainer([
    { index: 10, text: "row ten" },
    { index: 11, text: "row eleven" },
    { index: 12, text: "row twelve" },
  ]);
  const lastRow = container.querySelector('[data-index="12"]') as Element;

  container.getBoundingClientRect = () => ({ bottom: containerBottom, top: 100 }) as unknown as DOMRect;
  lastRow.getBoundingClientRect = () => ({ bottom: lastRowBottom }) as DOMRect;

  return container;
};

describe("getBottomDragClampTarget", () => {
  it("returns undefined when there is no selection range", () => {
    const container = buildClampContainer();
    const selection = makeDirectionalSelection({
      anchor: getRowTextNode(container, 11),
      anchorOffset: 2,
      focus: getRowTextNode(container, 10),
      focusOffset: 0,
    });

    Object.defineProperty(selection, "rangeCount", { value: 0 });

    expect(getBottomDragClampTarget({ container, pointerY: 600, selection })).toBeUndefined();
  });

  it("clamps to the last row when the pointer is below and the focus flipped above the anchor", () => {
    const container = buildClampContainer();
    const selection = makeDirectionalSelection({
      anchor: getRowTextNode(container, 11),
      anchorOffset: 2,
      focus: getRowTextNode(container, 10),
      focusOffset: 0,
    });
    const lastRow = container.querySelector('[data-index="12"]') as Element;

    expect(getBottomDragClampTarget({ container, pointerY: 600, selection })).toEqual({
      node: lastRow,
      offset: lastRow.childNodes.length,
    });
  });

  it("clamps inside the viewer after the pointer passes the last mounted row", () => {
    const container = buildClampContainer({ lastRowBottom: 480 });
    const selection = makeDirectionalSelection({
      anchor: getRowTextNode(container, 11),
      anchorOffset: 2,
      focus: getRowTextNode(container, 10),
      focusOffset: 0,
    });
    const lastRow = container.querySelector('[data-index="12"]') as Element;

    expect(getBottomDragClampTarget({ container, pointerY: 490, selection })).toEqual({
      node: lastRow,
      offset: lastRow.childNodes.length,
    });
  });

  it("does not clamp inside the viewer while the last mounted row continues below it", () => {
    const container = buildClampContainer();
    const selection = makeDirectionalSelection({
      anchor: getRowTextNode(container, 11),
      anchorOffset: 2,
      focus: getRowTextNode(container, 10),
      focusOffset: 0,
    });

    expect(getBottomDragClampTarget({ container, pointerY: 490, selection })).toBeUndefined();
  });

  it("clamps to the last row when the pointer is below and the focus left the rows", () => {
    const container = buildClampContainer();
    const outside = document.createElement("div");

    outside.textContent = "outside";
    document.body.append(outside);

    const selection = makeDirectionalSelection({
      anchor: getRowTextNode(container, 11),
      anchorOffset: 2,
      focus: outside.firstChild as Node,
      focusOffset: 0,
    });
    const lastRow = container.querySelector('[data-index="12"]') as Element;

    expect(getBottomDragClampTarget({ container, pointerY: 600, selection })).toEqual({
      node: lastRow,
      offset: lastRow.childNodes.length,
    });
  });

  it("does not clamp an upward selection when the pointer is above the container", () => {
    const container = buildClampContainer();
    const selection = makeDirectionalSelection({
      anchor: getRowTextNode(container, 11),
      anchorOffset: 2,
      focus: getRowTextNode(container, 10),
      focusOffset: 0,
    });

    expect(getBottomDragClampTarget({ container, pointerY: 50, selection })).toBeUndefined();
  });

  it("clamps a same-row focus inversion when the pointer is below the container", () => {
    const container = buildClampContainer();
    const node = getRowTextNode(container, 10);
    const selection = makeDirectionalSelection({
      anchor: node,
      anchorOffset: 4,
      focus: node,
      focusOffset: 0,
    });
    const lastRow = container.querySelector('[data-index="12"]') as Element;

    expect(getBottomDragClampTarget({ container, pointerY: 600, selection })).toEqual({
      node: lastRow,
      offset: lastRow.childNodes.length,
    });
  });

  it("follows the mounted edge for a forward selection while the pointer is below the container", () => {
    const container = buildClampContainer();
    const selection = makeDirectionalSelection({
      anchor: getRowTextNode(container, 10),
      anchorOffset: 2,
      focus: getRowTextNode(container, 11),
      focusOffset: 3,
    });
    const lastRow = container.querySelector('[data-index="12"]') as Element;

    expect(getBottomDragClampTarget({ container, pointerY: 600, selection })).toEqual({
      node: lastRow,
      offset: lastRow.childNodes.length,
    });
  });

  it("returns undefined when the focus already sits at the clamp target", () => {
    const container = buildClampContainer();
    const lastRow = container.querySelector('[data-index="12"]') as Element;
    const selection = makeDirectionalSelection({
      anchor: getRowTextNode(container, 12),
      anchorOffset: 2,
      focus: lastRow,
      focusOffset: lastRow.childNodes.length,
    });

    expect(getBottomDragClampTarget({ container, pointerY: 600, selection })).toBeUndefined();
  });

  it("returns undefined when the anchor is not inside a row", () => {
    const container = buildClampContainer();
    const outside = document.createElement("div");

    outside.textContent = "outside";
    document.body.append(outside);

    const selection = makeDirectionalSelection({
      anchor: outside.firstChild as Node,
      anchorOffset: 0,
      focus: getRowTextNode(container, 10),
      focusOffset: 0,
    });

    expect(getBottomDragClampTarget({ container, pointerY: 600, selection })).toBeUndefined();
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
