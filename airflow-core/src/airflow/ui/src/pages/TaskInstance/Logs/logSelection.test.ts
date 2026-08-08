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
import { createElement } from "react";
import { afterEach, describe, expect, it } from "vitest";

import {
  extractSelectedLogText,
  getEntryText,
  getSelectionPinnedRows,
  getSelectionRowRange,
  mergePinnedIndexes,
} from "./logSelection";

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

describe("getSelectionRowRange", () => {
  it("returns the row range when both boundaries are inside log rows", () => {
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

    expect(getSelectionRowRange(selection, container)).toEqual({ end: 7, start: 2 });
  });

  it("returns undefined for a collapsed selection", () => {
    const container = buildLogContainer([{ index: 0, text: "line 0" }]);
    const node = getRowTextNode(container, 0);

    expect(
      getSelectionRowRange(
        selectBetween({ end: node, endOffset: 2, start: node, startOffset: 2 }),
        container,
      ),
    ).toBeUndefined();
  });

  it("returns undefined when a boundary is outside the container", () => {
    const container = buildLogContainer([{ index: 0, text: "line 0" }]);
    const outside = document.createElement("div");

    outside.setAttribute("data-index", "99");
    outside.textContent = "not a log line";
    document.body.append(outside);

    const selection = selectBetween({
      end: getRowTextNode(container, 0),
      endOffset: 3,
      start: outside.firstChild as Node,
      startOffset: 0,
    });

    expect(getSelectionRowRange(selection, container)).toBeUndefined();
  });

  it("returns undefined when a boundary degraded to the container itself", () => {
    const container = buildLogContainer([
      { index: 0, text: "line 0" },
      { index: 1, text: "line 1" },
    ]);
    const range = document.createRange();

    range.setStart(container, 0);
    range.setEnd(getRowTextNode(container, 1), 3);

    expect(getSelectionRowRange(makeSelection(range), container)).toBeUndefined();
  });

  it("returns undefined for a null selection", () => {
    const container = buildLogContainer([{ index: 0, text: "line 0" }]);

    expect(getSelectionRowRange(null, container)).toBeUndefined();
  });
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

describe("extractSelectedLogText", () => {
  it("returns undefined for a selection within a single row", () => {
    const container = buildLogContainer([{ index: 0, text: "hello world" }]);
    const node = getRowTextNode(container, 0);

    expect(
      extractSelectedLogText({
        container,
        getRowText: () => "",
        selection: selectBetween({ end: node, endOffset: 5, start: node, startOffset: 0 }),
      }),
    ).toBeUndefined();
  });

  it("returns undefined when every selected row is mounted", () => {
    const container = buildLogContainer([
      { index: 0, text: "line 0" },
      { index: 1, text: "line 1" },
      { index: 2, text: "line 2" },
    ]);

    expect(
      extractSelectedLogText({
        container,
        getRowText: () => "",
        selection: selectBetween({
          end: getRowTextNode(container, 2),
          endOffset: 3,
          start: getRowTextNode(container, 0),
          startOffset: 0,
        }),
      }),
    ).toBeUndefined();
  });

  it("rebuilds unmounted middle rows from log data, keeping partial boundary rows", () => {
    const container = buildLogContainer([
      { index: 10, text: "hello world" },
      { index: 11, text: "mounted middle" },
      { index: 40, text: "foo bar" },
    ]);
    const selection = selectBetween({
      end: getRowTextNode(container, 40),
      endOffset: 3,
      start: getRowTextNode(container, 10),
      startOffset: 6,
    });

    const text = extractSelectedLogText({
      container,
      getRowText: (index) => `line ${index}`,
      selection,
    });

    const middleLines = Array.from({ length: 29 }, (_, offset) => `line ${offset + 11}`);

    expect(text).toBe(["world", ...middleLines, "foo"].join("\n"));
  });

  it("excludes copy-excluded line-number elements from boundary rows", () => {
    const container = document.createElement("div");
    const buildRow = (index: number, messageText: string) => {
      const row = document.createElement("div");

      row.setAttribute("data-index", String(index));

      const lineNumberLink = document.createElement("a");

      lineNumberLink.setAttribute("data-copy-exclude", "");
      lineNumberLink.textContent = String(index);

      const message = document.createElement("span");

      message.textContent = messageText;
      row.append(lineNumberLink, message);
      container.append(row);

      return message;
    };
    const firstMessage = buildRow(10, "hello world");

    buildRow(11, "mounted middle");

    const lastMessage = buildRow(40, "foo bar");

    document.body.append(container);

    const selection = selectBetween({
      end: lastMessage.firstChild as Node,
      endOffset: 3,
      start: firstMessage.firstChild as Node,
      startOffset: 6,
    });

    const text = extractSelectedLogText({
      container,
      getRowText: (index) => `line ${index}`,
      selection,
    });

    const middleLines = Array.from({ length: 29 }, (_, offset) => `line ${offset + 11}`);

    expect(text).toBe(["world", ...middleLines, "foo"].join("\n"));
  });

  it("returns undefined for multi-range selections", () => {
    const container = buildLogContainer([
      { index: 0, text: "line 0" },
      { index: 5, text: "line 5" },
    ]);
    const range = document.createRange();

    range.setStart(getRowTextNode(container, 0), 0);
    range.setEnd(getRowTextNode(container, 5), 3);

    const selection = { getRangeAt: () => range, isCollapsed: false, rangeCount: 2 } as unknown as Selection;

    expect(extractSelectedLogText({ container, getRowText: () => "", selection })).toBeUndefined();
  });

  it("returns undefined when the selection cannot be mapped to rows", () => {
    const container = buildLogContainer([
      { index: 0, text: "line 0" },
      { index: 5, text: "line 5" },
    ]);
    const range = document.createRange();

    range.setStart(container, 0);
    range.setEnd(getRowTextNode(container, 5), 3);

    expect(
      extractSelectedLogText({ container, getRowText: () => "", selection: makeSelection(range) }),
    ).toBeUndefined();
  });
});

describe("getEntryText", () => {
  it("rebuilds collapsed group headers with the collapsed marker", () => {
    expect(getEntryText({ element: "Pre Execute", group: { id: 0, level: 0, type: "header" } })).toBe(
      "▶ Pre Execute",
    );
  });

  it("rebuilds expanded group headers with the expanded marker", () => {
    expect(
      getEntryText({ element: "Pre Execute", group: { id: 0, level: 0, type: "header" } }, new Set([0])),
    ).toBe("▼ Pre Execute");
  });

  it("returns non-header string elements directly", () => {
    expect(getEntryText({ element: "Pre Execute" })).toBe("Pre Execute");
  });

  it("prefers getPlainText over innerText for rendered log lines", () => {
    const entry = {
      element: createElement("span", undefined, "jsx text"),
      getPlainText: () => "canonical text",
    };

    expect(getEntryText(entry)).toBe("canonical text");
  });

  it("falls back to innerText when getPlainText is absent", () => {
    expect(getEntryText({ element: createElement("span", undefined, "jsx text") })).toBe("jsx text");
  });
});
