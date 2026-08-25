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
import "@testing-library/jest-dom";
import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import { beforeAll, describe, expect, it, vi } from "vitest";

import { AppWrapper } from "src/utils/AppWrapper";

const ITEM_HEIGHT = 20;

beforeAll(() => {
  Object.defineProperty(HTMLElement.prototype, "offsetHeight", {
    value: ITEM_HEIGHT,
  });
  Object.defineProperty(HTMLElement.prototype, "offsetWidth", {
    value: 800,
  });
});

const waitForLogs = async () => {
  await waitFor(() => expect(screen.getByTestId("virtualized-list")).toBeInTheDocument());

  // Wait for virtualized items to be rendered - they might not all be visible initially
  // Items can have either virtualized-item- or group-header- testid prefixes
  await waitFor(() => {
    const virtualizedList = screen.getByTestId("virtualized-list");
    const virtualizedItems = virtualizedList.querySelectorAll(
      '[data-testid^="virtualized-item-"], [data-testid^="group-header-"]',
    );

    expect(virtualizedItems.length).toBeGreaterThan(0);
  });

  fireEvent.scroll(screen.getByTestId("virtualized-list"), { target: { scrollTop: ITEM_HEIGHT * 2 } });
};

const makeClipboardData = () => {
  const store = new Map<string, string>();

  return {
    getData: (type: string) => store.get(type) ?? "",
    setData: (type: string, value: string) => store.set(type, value),
  };
};

const dispatchCopy = (clipboardData: ReturnType<typeof makeClipboardData>) => {
  const copyEvent = new Event("copy", { bubbles: true, cancelable: true });

  Object.defineProperty(copyEvent, "clipboardData", { value: clipboardData });
  document.dispatchEvent(copyEvent);

  return copyEvent;
};

const findRow = (text: string) => {
  const container = screen.getByTestId("virtual-scroll-container");

  return [...container.querySelectorAll("[data-index]")].find((row) =>
    row.textContent.includes(text),
  ) as HTMLElement;
};

const getRowCopyText = (row: HTMLElement) => {
  const clone = row.cloneNode(true) as HTMLElement;

  for (const element of clone.querySelectorAll("[data-copy-exclude]")) {
    element.remove();
  }

  return clone.textContent;
};

const withFakeSelection = <T,>(selection: Selection, callback: () => T): T => {
  const getSelectionSpy = vi.spyOn(document, "getSelection").mockReturnValue(selection);
  const result = callback();

  getSelectionSpy.mockRestore();

  return result;
};

describe("Copy across virtualized rows", () => {
  it("rebuilds a removed grouped row from log data and strips line numbers", async () => {
    render(
      <AppWrapper initialEntries={["/dags/log_grouping/runs/manual__2025-02-18T12:19/tasks/ti_context"]} />,
    );
    await waitForLogs();

    fireEvent.click(screen.getByTestId("summary-Pre Execute"));
    await waitFor(() => expect(screen.getByText(/DAG bundles loaded/iu)).toBeInTheDocument());

    const firstRow = findRow("Task started");
    const middleRow = findRow("DAG bundles loaded");
    const lastRow = findRow("Done. Returned value was: None");

    expect(firstRow).toBeDefined();
    expect(middleRow).toBeDefined();
    expect(lastRow).toBeDefined();

    middleRow.remove();

    const range = document.createRange();

    range.setStart(firstRow, 0);
    range.setEnd(lastRow, lastRow.childNodes.length);

    const selection = { getRangeAt: () => range, isCollapsed: false, rangeCount: 1 } as unknown as Selection;
    const clipboardData = makeClipboardData();
    const copyEvent = withFakeSelection(selection, () => dispatchCopy(clipboardData));

    expect(copyEvent.defaultPrevented).toBe(true);

    const lines = clipboardData.getData("text/plain").split("\n");

    expect(lines[0]).not.toMatch(/^\d/u);
    expect(lines[0]).toContain("Task started");
    expect(lines).toContainEqual(
      expect.stringMatching(/^\[.+\] INFO - DAG bundles loaded: dags-folder, example_dags$/u),
    );
  });

  it("rebuilds a removed ungrouped row from log data", async () => {
    render(
      <AppWrapper initialEntries={["/dags/log_grouping/runs/manual__2025-02-18T12:19/tasks/ti_context"]} />,
    );
    await waitForLogs();

    const firstRow = findRow("Log message source details");
    const middleRow = findRow("Task started");
    const lastRow = findRow("Done. Returned value was: None");

    expect(firstRow).toBeDefined();
    expect(middleRow).toBeDefined();
    expect(lastRow).toBeDefined();

    middleRow.remove();

    const range = document.createRange();

    range.setStart(firstRow, 0);
    range.setEnd(lastRow, lastRow.childNodes.length);

    const selection = { getRangeAt: () => range, isCollapsed: false, rangeCount: 1 } as unknown as Selection;
    const clipboardData = makeClipboardData();
    const copyEvent = withFakeSelection(selection, () => dispatchCopy(clipboardData));

    expect(copyEvent.defaultPrevented).toBe(true);
    expect(clipboardData.getData("text/plain").split("\n")).toContainEqual(
      expect.stringMatching(/^\[.+\] INFO - Task started$/u),
    );
  });

  it("rebuilds middle rows with the exact text mounted rows show on screen", async () => {
    render(
      <AppWrapper initialEntries={["/dags/log_grouping/runs/manual__2025-02-18T12:19/tasks/ti_context"]} />,
    );
    await waitForLogs();

    const firstRow = findRow("Log message source details");
    const taskStartedRow = findRow("Task started");
    const headerRow = findRow("Pre Execute");
    const lastRow = findRow("Done. Returned value was: None");

    const taskStartedScreenText = getRowCopyText(taskStartedRow);
    const headerScreenText = getRowCopyText(headerRow);

    expect(taskStartedScreenText).toMatch(/^\[\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\] INFO - Task started$/u);
    expect(headerScreenText).toBe("▶ Pre Execute");

    taskStartedRow.remove();

    const range = document.createRange();

    range.setStart(firstRow, 0);
    range.setEnd(lastRow, lastRow.childNodes.length);

    const selection = { getRangeAt: () => range, isCollapsed: false, rangeCount: 1 } as unknown as Selection;
    const clipboardData = makeClipboardData();

    withFakeSelection(selection, () => dispatchCopy(clipboardData));

    const lines = clipboardData.getData("text/plain").split("\n");

    expect(lines[0]).toBe("▶ Log message source details");
    expect(lines).toContain(taskStartedScreenText);
    expect(lines).toContain(headerScreenText);
  });

  it("copies the expanded marker for expanded group headers", async () => {
    render(
      <AppWrapper initialEntries={["/dags/log_grouping/runs/manual__2025-02-18T12:19/tasks/ti_context"]} />,
    );
    await waitForLogs();

    fireEvent.click(screen.getByTestId("summary-Pre Execute"));
    await waitFor(() => expect(getRowCopyText(findRow("Pre Execute"))).toBe("▼ Pre Execute"));

    const firstRow = findRow("Log message source details");
    const headerRow = findRow("Pre Execute");
    const taskStartedRow = findRow("Task started");
    const lastRow = findRow("Done. Returned value was: None");

    taskStartedRow.remove();
    headerRow.remove();

    const range = document.createRange();

    range.setStart(firstRow, 0);
    range.setEnd(lastRow, lastRow.childNodes.length);

    const selection = { getRangeAt: () => range, isCollapsed: false, rangeCount: 1 } as unknown as Selection;
    const clipboardData = makeClipboardData();

    withFakeSelection(selection, () => dispatchCopy(clipboardData));

    expect(clipboardData.getData("text/plain").split("\n")).toContain("▼ Pre Execute");
  });

  it("leaves single-row selections to native copy", async () => {
    render(
      <AppWrapper initialEntries={["/dags/log_grouping/runs/manual__2025-02-18T12:19/tasks/ti_context"]} />,
    );
    await waitForLogs();

    const row = findRow("Task started");

    expect(row).toBeDefined();

    const range = document.createRange();

    range.setStart(row, 0);
    range.setEnd(row, row.childNodes.length);

    const selection = { getRangeAt: () => range, isCollapsed: false, rangeCount: 1 } as unknown as Selection;
    const clipboardData = makeClipboardData();
    const copyEvent = withFakeSelection(selection, () => dispatchCopy(clipboardData));

    expect(copyEvent.defaultPrevented).toBe(false);
    expect(clipboardData.getData("text/plain")).toBe("");
  });
});

describe("Selection pinning across scrolling", () => {
  it("keeps the selection-anchor row mounted after scrolling it out of the render window", async () => {
    render(
      <AppWrapper initialEntries={["/dags/log_grouping/runs/manual__2025-02-18T12:19/tasks/generate"]} />,
    );
    await waitForLogs();

    fireEvent.click(screen.getByTestId("summary-Pre task execution logs"));
    await waitFor(() => expect(screen.getByText(/starting attempt 1 of 3/iu)).toBeInTheDocument());

    const anchorRow = findRow("Starting attempt 1 of 3");
    const anchorIndex = Number(anchorRow.getAttribute("data-index"));
    const neighborIndex = anchorIndex + 1;
    const textNode = anchorRow.querySelector("span")?.firstChild as Node;
    const range = document.createRange();

    range.setStart(textNode, 0);
    range.setEnd(textNode, 0);

    const selection = { getRangeAt: () => range, isCollapsed: true, rangeCount: 1 } as unknown as Selection;

    withFakeSelection(selection, () => {
      document.dispatchEvent(new Event("selectionchange"));
    });

    const container = screen.getByTestId("virtual-scroll-container");

    fireEvent.scroll(container, { target: { scrollTop: ITEM_HEIGHT * (anchorIndex + 15) } });

    await waitFor(() => {
      expect(container.querySelector(`[data-index="${neighborIndex}"]`)).toBeNull();
    });
    expect(container.querySelector(`[data-index="${anchorIndex}"]`)).not.toBeNull();
  });

  it("unpins once the selection is cleared", async () => {
    render(
      <AppWrapper initialEntries={["/dags/log_grouping/runs/manual__2025-02-18T12:19/tasks/generate"]} />,
    );
    await waitForLogs();

    fireEvent.click(screen.getByTestId("summary-Pre task execution logs"));
    await waitFor(() => expect(screen.getByText(/starting attempt 1 of 3/iu)).toBeInTheDocument());

    const anchorRow = findRow("Starting attempt 1 of 3");
    const anchorIndex = Number(anchorRow.getAttribute("data-index"));
    const textNode = anchorRow.querySelector("span")?.firstChild as Node;
    const range = document.createRange();

    range.setStart(textNode, 0);
    range.setEnd(textNode, 0);

    const selection = { getRangeAt: () => range, isCollapsed: true, rangeCount: 1 } as unknown as Selection;

    withFakeSelection(selection, () => {
      document.dispatchEvent(new Event("selectionchange"));
    });

    const noSelection = null as unknown as Selection;

    withFakeSelection(noSelection, () => {
      document.dispatchEvent(new Event("selectionchange"));
    });

    const container = screen.getByTestId("virtual-scroll-container");

    fireEvent.scroll(container, { target: { scrollTop: ITEM_HEIGHT * (anchorIndex + 15) } });

    await waitFor(() => {
      expect(container.querySelector(`[data-index="${anchorIndex}"]`)).toBeNull();
    });
  });
});

describe("Downward drag selection", () => {
  it("coalesces events and extends the selection to the mounted bottom row", async () => {
    render(
      <AppWrapper initialEntries={["/dags/log_grouping/runs/manual__2025-02-18T12:19/tasks/ti_context"]} />,
    );
    await waitForLogs();

    const container = screen.getByTestId("virtual-scroll-container");
    const rows = container.querySelectorAll<HTMLElement>("[data-index]");
    const anchorRow = rows[0] as HTMLElement;
    const lastRow = rows[rows.length - 1] as HTMLElement;
    const extend = vi.fn();
    const range = document.createRange();

    range.selectNodeContents(anchorRow);

    const selection = {
      anchorNode: anchorRow,
      extend,
      focusNode: anchorRow,
      focusOffset: 0,
      getRangeAt: () => range,
      rangeCount: 1,
    } as unknown as Selection;
    const animationFrames = new Array<FrameRequestCallback>();
    const getSelectionSpy = vi.spyOn(document, "getSelection").mockReturnValue(selection);
    const requestAnimationFrameSpy = vi
      .spyOn(globalThis, "requestAnimationFrame")
      .mockImplementation((callback) => {
        animationFrames.push(callback);

        return animationFrames.length;
      });
    const cancelAnimationFrameSpy = vi
      .spyOn(globalThis, "cancelAnimationFrame")
      .mockImplementation(() => undefined);

    container.getBoundingClientRect = () => ({ bottom: 500 }) as DOMRect;
    lastRow.getBoundingClientRect = () => ({ bottom: 480 }) as DOMRect;

    fireEvent.pointerDown(anchorRow, { button: 0, clientY: 200, pointerType: "mouse" });
    fireEvent.pointerMove(document, { clientY: 490, pointerType: "mouse" });
    document.dispatchEvent(new Event("selectionchange"));
    fireEvent.scroll(container);

    expect(animationFrames).toHaveLength(1);
    animationFrames.shift()?.(0);
    expect(extend).toHaveBeenCalledWith(lastRow, lastRow.childNodes.length);

    fireEvent.scroll(container);
    expect(animationFrames).toHaveLength(1);
    animationFrames.shift()?.(1);
    expect(extend).toHaveBeenCalledTimes(2);

    fireEvent.scroll(container);
    const pendingAnimationFrame = animationFrames.shift();

    fireEvent.pointerUp(document, { pointerType: "mouse" });
    expect(cancelAnimationFrameSpy).toHaveBeenCalledWith(1);

    pendingAnimationFrame?.(2);
    expect(extend).toHaveBeenCalledTimes(2);

    getSelectionSpy.mockRestore();
    requestAnimationFrameSpy.mockRestore();
    cancelAnimationFrameSpy.mockRestore();
  });

  it("only activates for downward primary-mouse drags starting in a log row", async () => {
    render(
      <AppWrapper initialEntries={["/dags/log_grouping/runs/manual__2025-02-18T12:19/tasks/ti_context"]} />,
    );
    await waitForLogs();

    const container = screen.getByTestId("virtual-scroll-container");
    const anchorRow = container.querySelector<HTMLElement>("[data-index]") as HTMLElement;
    const rows = container.querySelectorAll<HTMLElement>("[data-index]");
    const lastRow = rows[rows.length - 1] as HTMLElement;
    const requestAnimationFrameSpy = vi
      .spyOn(globalThis, "requestAnimationFrame")
      .mockImplementation(() => 1);

    container.getBoundingClientRect = () => ({ bottom: 500 }) as DOMRect;
    lastRow.getBoundingClientRect = () => ({ bottom: 700 }) as DOMRect;

    fireEvent.pointerDown(container, { button: 0, clientY: 200, pointerType: "mouse" });
    fireEvent.pointerMove(document, { clientY: 600, pointerType: "mouse" });
    expect(requestAnimationFrameSpy).not.toHaveBeenCalled();

    fireEvent.pointerDown(anchorRow, { button: 2, clientY: 200, pointerType: "mouse" });
    fireEvent.pointerMove(document, { clientY: 600, pointerType: "mouse" });
    expect(requestAnimationFrameSpy).not.toHaveBeenCalled();

    fireEvent.pointerDown(anchorRow, { button: 0, clientY: 200, pointerType: "touch" });
    fireEvent.pointerMove(document, { clientY: 600, pointerType: "touch" });
    expect(requestAnimationFrameSpy).not.toHaveBeenCalled();

    fireEvent.pointerDown(anchorRow, { button: 0, clientY: 200, pointerType: "mouse" });
    fireEvent.pointerMove(document, { clientY: 50, pointerType: "mouse" });
    fireEvent.scroll(container);
    expect(requestAnimationFrameSpy).not.toHaveBeenCalled();

    fireEvent.pointerUp(document, { pointerType: "mouse" });
    requestAnimationFrameSpy.mockRestore();
  });
});
