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
import type { PropsWithChildren, RefObject } from "react";
import { createRef } from "react";

import { act, fireEvent, render, screen } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";

import { ROW_HEIGHT } from "src/layouts/Details/Grid/constants";
import type { GridTask } from "src/layouts/Details/Grid/utils";

import { Wrapper } from "src/utils/Wrapper";

import { GanttTimeline } from "./GanttTimeline";
import type { GanttDataItem } from "./utils";

// Mock the virtualizer so all items render regardless of scroll-container dimensions
// (happy-dom does not perform layout, so getScrollElement always has height 0).
vi.mock("@tanstack/react-virtual", () => ({
  useVirtualizer: ({ count, estimateSize }: { count: number; estimateSize: () => number }) => ({
    getTotalSize: () => count * estimateSize(),
    getVirtualItems: () =>
      Array.from({ length: count }, (_, index) => ({
        index,
        key: String(index),
        lane: 0,
        size: estimateSize(),
        start: index * estimateSize(),
      })),
  }),
}));

let mockParams: Record<string, string | undefined> = {};
let mockSearchParams = new URLSearchParams();

vi.mock("react-router-dom", async (importOriginal) => {
  const original = await importOriginal();

  return {
    ...(original as object),
    useParams: () => mockParams,
    useSearchParams: () => [mockSearchParams, vi.fn()] as const,
  };
});

// Group membership is read from the Dag structure, which this test does not stand up.
vi.mock("src/queries/useIsLoopGroup", () => ({
  useGroupMembers: (groupId?: string) =>
    groupId === "refine" ? new Set(["refine", "refine.improve", "refine.evaluate"]) : new Set<string>(),
}));

const TestWrapper = ({ children }: PropsWithChildren) => <Wrapper>{children}</Wrapper>;

// Shared time range: 10:00 → 10:10 UTC on 2024-03-14
const MIN_MS = new Date("2024-03-14T10:00:00Z").getTime();
const MAX_MS = new Date("2024-03-14T10:10:00Z").getTime();

const BASE_NODE: GridTask = {
  depth: 0,
  id: "task_1",
  is_mapped: false,
  label: "task_1",
};

const makeScrollRef = (): RefObject<HTMLDivElement | null> => {
  const ref = createRef<HTMLDivElement | null>();

  (ref as { current: HTMLDivElement | null }).current = document.createElement("div");

  return ref;
};

const defaultProps = {
  dagId: "test_dag",
  flatNodes: [BASE_NODE],
  gridSummaries: [],
  maxMs: MAX_MS,
  minMs: MIN_MS,
  rowSegments: [[]] as Array<Array<GanttDataItem>>,
  runId: "run_1",
  virtualizerScrollPaddingStart: ROW_HEIGHT,
};

const buildSegment = (taskInstanceId: string, start: number, end: number): GanttDataItem => ({
  state: "success",
  taskId: "task_1",
  taskInstanceId,
  tryNumber: 1,
  x: [start, end],
  y: "task_1",
});

const passSegment = (taskId: string, iteration: number): GanttDataItem => ({
  loopIteration: { iteration, loop_id: "refine" },
  state: "success",
  taskId,
  taskInstanceId: `${taskId}-${iteration}`,
  tryNumber: 1,
  x: [MIN_MS, MAX_MS],
  y: taskId,
});

const PASS_MEMBERS = ["refine.improve", "refine.evaluate"];

describe("GanttTimeline segment bars", () => {
  afterEach(() => {
    vi.unstubAllGlobals();
  });

  it.each([1, 3])("shows loop iteration %s in the hovered bar's tooltip", async (iteration) => {
    const segment: GanttDataItem = {
      loopIteration: { iteration, loop_id: "loop" },
      regionId: "11111111-1111-1111-1111-111111111111",
      regionIndex: 99,
      state: "success",
      taskId: "task_1",
      taskInstanceId: "retained-execution",
      tryNumber: 1,
      x: [MIN_MS, MAX_MS],
      y: "task_1",
    };

    render(
      <GanttTimeline {...defaultProps} rowSegments={[[segment]]} scrollContainerRef={makeScrollRef()} />,
      { wrapper: TestWrapper },
    );

    fireEvent.pointerMove(screen.getByRole("link"), { pointerType: "mouse" });

    // The loop's name is not repeated here: the group it belongs to is already on screen.
    expect(await screen.findByText(`taskInstance.iteration: ${iteration}`)).toBeInTheDocument();
  });

  it.each([
    { name: "ordinary task", node: BASE_NODE, regionId: undefined, regionIndex: undefined },
    {
      name: "mapped aggregate",
      node: { ...BASE_NODE, is_mapped: true },
      regionId: "mapping",
      regionIndex: 2,
    },
    { name: "group aggregate", node: { ...BASE_NODE, isGroup: true }, regionId: "loop", regionIndex: 2 },
  ])("does not label $name as one loop iteration", async ({ node, regionId, regionIndex }) => {
    const segment: GanttDataItem = {
      regionId,
      regionIndex,
      state: "success",
      taskId: "task_1",
      tryNumber: 1,
      x: [MIN_MS, MAX_MS],
      y: "task_1",
    };

    render(
      <GanttTimeline
        {...defaultProps}
        flatNodes={[node]}
        rowSegments={[[segment]]}
        scrollContainerRef={makeScrollRef()}
      />,
      { wrapper: TestWrapper },
    );

    fireEvent.pointerMove(screen.getByRole("link"), { pointerType: "mouse" });

    expect(await screen.findByText("taskId: task_1")).toBeInTheDocument();
    expect(screen.queryByText(/taskInstance.iteration/u)).not.toBeInTheDocument();
  });

  it.each([
    { expectedIcons: 1, name: "joins adjacent same-try segments of one execution", secondId: "execution-a" },
    {
      expectedIcons: 2,
      name: "keeps adjacent same-try segments of different executions separate",
      secondId: "execution-b",
    },
  ])("$name", ({ expectedIcons, secondId }) => {
    const middle = MIN_MS + (MAX_MS - MIN_MS) / 2;

    render(
      <GanttTimeline
        {...defaultProps}
        rowSegments={[[buildSegment("execution-a", MIN_MS, middle), buildSegment(secondId, middle, MAX_MS)]]}
        scrollContainerRef={makeScrollRef()}
      />,
      { wrapper: TestWrapper },
    );

    expect(screen.getAllByRole("link").filter((link) => link.querySelector("svg") !== null)).toHaveLength(
      expectedIcons,
    );
  });

  it("renders a single execution bar when only start_date is present", () => {
    const executionSegment: GanttDataItem = {
      queued_when: null,
      scheduled_when: null,
      state: "success",
      taskId: "task_1",
      tryNumber: 1,
      x: [new Date("2024-03-14T10:00:00Z").getTime(), new Date("2024-03-14T10:05:00Z").getTime()],
      y: "task_1",
    };

    render(
      <GanttTimeline
        {...defaultProps}
        rowSegments={[[executionSegment]]}
        scrollContainerRef={makeScrollRef()}
      />,
      { wrapper: TestWrapper },
    );

    // One link per bar — only the execution bar should appear
    expect(screen.getAllByRole("link")).toHaveLength(1);
  });

  it("renders two bars (queued + execution) when queued_dttm is present", () => {
    const queuedSegment: GanttDataItem = {
      queued_when: "2024-03-14T09:59:00Z",
      scheduled_when: null,
      state: "queued",
      taskId: "task_1",
      tryNumber: 1,
      x: [new Date("2024-03-14T09:59:00Z").getTime(), new Date("2024-03-14T10:00:00Z").getTime()],
      y: "task_1",
    };
    const executionSegment: GanttDataItem = {
      queued_when: "2024-03-14T09:59:00Z",
      scheduled_when: null,
      state: "success",
      taskId: "task_1",
      tryNumber: 1,
      x: [new Date("2024-03-14T10:00:00Z").getTime(), new Date("2024-03-14T10:05:00Z").getTime()],
      y: "task_1",
    };
    const segments = [queuedSegment, executionSegment];

    render(
      <GanttTimeline {...defaultProps} rowSegments={[segments]} scrollContainerRef={makeScrollRef()} />,
      { wrapper: TestWrapper },
    );

    expect(screen.getAllByRole("link")).toHaveLength(2);
  });

  it("renders three bars (scheduled + queued + execution) when both scheduled_dttm and queued_dttm are present", () => {
    const scheduledSegment: GanttDataItem = {
      queued_when: "2024-03-14T09:59:00Z",
      scheduled_when: "2024-03-14T09:58:00Z",
      state: "scheduled",
      taskId: "task_1",
      tryNumber: 1,
      x: [new Date("2024-03-14T09:58:00Z").getTime(), new Date("2024-03-14T09:59:00Z").getTime()],
      y: "task_1",
    };
    const queuedSegment: GanttDataItem = {
      queued_when: "2024-03-14T09:59:00Z",
      scheduled_when: "2024-03-14T09:58:00Z",
      state: "queued",
      taskId: "task_1",
      tryNumber: 1,
      x: [new Date("2024-03-14T09:59:00Z").getTime(), new Date("2024-03-14T10:00:00Z").getTime()],
      y: "task_1",
    };
    const executionSegment: GanttDataItem = {
      queued_when: "2024-03-14T09:59:00Z",
      scheduled_when: "2024-03-14T09:58:00Z",
      state: "success",
      taskId: "task_1",
      tryNumber: 1,
      x: [new Date("2024-03-14T10:00:00Z").getTime(), new Date("2024-03-14T10:05:00Z").getTime()],
      y: "task_1",
    };
    const segments = [scheduledSegment, queuedSegment, executionSegment];

    render(
      <GanttTimeline {...defaultProps} rowSegments={[segments]} scrollContainerRef={makeScrollRef()} />,
      { wrapper: TestWrapper },
    );

    expect(screen.getAllByRole("link")).toHaveLength(3);
  });

  it("renders bars for multiple tasks independently", () => {
    const node2: GridTask = { depth: 0, id: "task_2", is_mapped: false, label: "task_2" };

    // task_1: scheduled + execution (2 bars)
    const t1Scheduled: GanttDataItem = {
      queued_when: null,
      scheduled_when: "2024-03-14T09:58:00Z",
      state: "scheduled",
      taskId: "task_1",
      tryNumber: 1,
      x: [new Date("2024-03-14T09:58:00Z").getTime(), new Date("2024-03-14T10:00:00Z").getTime()],
      y: "task_1",
    };
    const t1Exec: GanttDataItem = {
      queued_when: null,
      scheduled_when: "2024-03-14T09:58:00Z",
      state: "failed",
      taskId: "task_1",
      tryNumber: 1,
      x: [new Date("2024-03-14T10:00:00Z").getTime(), new Date("2024-03-14T10:03:00Z").getTime()],
      y: "task_1",
    };

    // task_2: execution only (1 bar)
    const t2Exec: GanttDataItem = {
      queued_when: null,
      scheduled_when: null,
      state: "success",
      taskId: "task_2",
      tryNumber: 1,
      x: [new Date("2024-03-14T10:04:00Z").getTime(), new Date("2024-03-14T10:08:00Z").getTime()],
      y: "task_2",
    };

    render(
      <GanttTimeline
        {...defaultProps}
        flatNodes={[BASE_NODE, node2]}
        rowSegments={[[t1Scheduled, t1Exec], [t2Exec]]}
        scrollContainerRef={makeScrollRef()}
      />,
      { wrapper: TestWrapper },
    );

    // 2 bars for task_1 + 1 bar for task_2 = 3 total
    expect(screen.getAllByRole("link")).toHaveLength(3);
  });

  it("renders no bars when rowSegments is empty", () => {
    render(
      <GanttTimeline
        {...defaultProps}
        flatNodes={[BASE_NODE]}
        rowSegments={[[]]}
        scrollContainerRef={makeScrollRef()}
      />,
      { wrapper: TestWrapper },
    );

    expect(screen.queryAllByRole("link")).toHaveLength(0);
  });

  it("hides scheduled and queued bars whose pixel width is below MIN_SEGMENT_RENDER_PX (5 px)", () => {
    // Simulate a 1000 px-wide Gantt body via a ResizeObserver mock.
    // spanMs = MAX_MS − MIN_MS = 600 000 ms (10 min)
    // A 2-second bar → (2 000 / 600 000) * 1 000 ≈ 3.33 px  <  5 px threshold → hidden
    // A 5-minute execution bar → (300 000 / 600 000) * 1 000 = 500 px → visible

    let observerCallback: ResizeObserverCallback | undefined;

    vi.stubGlobal(
      "ResizeObserver",
      class MockResizeObserver {
        public constructor(cb: ResizeObserverCallback) {
          observerCallback = cb;
        }
        // eslint-disable-next-line @typescript-eslint/class-methods-use-this
        public disconnect() {
          /* empty */
        }
        // eslint-disable-next-line @typescript-eslint/class-methods-use-this
        public observe() {
          /* empty */
        }
        // eslint-disable-next-line @typescript-eslint/class-methods-use-this
        public unobserve() {
          /* empty */
        }
      },
    );

    const scheduledSegment: GanttDataItem = {
      queued_when: new Date(MIN_MS + 2000).toISOString(),
      scheduled_when: new Date(MIN_MS).toISOString(),
      state: "scheduled",
      taskId: "task_1",
      tryNumber: 1,
      // 2 s wide — below the 5 px threshold at 1000 px viewport
      x: [MIN_MS, MIN_MS + 2000],
      y: "task_1",
    };
    const queuedSegment: GanttDataItem = {
      queued_when: new Date(MIN_MS + 2000).toISOString(),
      scheduled_when: new Date(MIN_MS).toISOString(),
      state: "queued",
      taskId: "task_1",
      tryNumber: 1,
      // 2 s wide — below the 5 px threshold at 1000 px viewport
      x: [MIN_MS + 2000, MIN_MS + 4000],
      y: "task_1",
    };
    const executionSegment: GanttDataItem = {
      queued_when: new Date(MIN_MS + 2000).toISOString(),
      scheduled_when: new Date(MIN_MS).toISOString(),
      state: "success",
      taskId: "task_1",
      tryNumber: 1,
      // 5 min wide — well above threshold
      x: [MIN_MS + 4000, MIN_MS + 304_000],
      y: "task_1",
    };
    const segments = [scheduledSegment, queuedSegment, executionSegment];

    render(
      <GanttTimeline {...defaultProps} rowSegments={[segments]} scrollContainerRef={makeScrollRef()} />,
      { wrapper: TestWrapper },
    );

    // Fire the ResizeObserver callback to set bodyWidthPx = 1000, enabling the filter.
    act(() => {
      observerCallback?.([{ contentRect: { width: 1000 } } as ResizeObserverEntry], {} as ResizeObserver);
    });

    // Only the execution bar should be rendered; scheduled and queued are too narrow.
    expect(screen.getAllByRole("link")).toHaveLength(1);
  });

  it("hatches a task-group aggregate bar so it does not read as a single long task", () => {
    const groupNode: GridTask = {
      depth: 0,
      id: "group_1",
      is_mapped: false,
      isGroup: true,
      label: "group_1",
    };
    const groupSegment: GanttDataItem = {
      queued_when: null,
      scheduled_when: null,
      state: "success",
      taskId: "group_1",
      tryNumber: undefined,
      x: [new Date("2024-03-14T10:00:00Z").getTime(), new Date("2024-03-14T10:05:00Z").getTime()],
      y: "group_1",
    };
    const leafSegment: GanttDataItem = {
      queued_when: null,
      scheduled_when: null,
      state: "success",
      taskId: "task_1",
      tryNumber: 1,
      x: [new Date("2024-03-14T10:00:00Z").getTime(), new Date("2024-03-14T10:05:00Z").getTime()],
      y: "task_1",
    };

    render(
      <GanttTimeline
        {...defaultProps}
        flatNodes={[groupNode, BASE_NODE]}
        rowSegments={[[groupSegment], [leafSegment]]}
        scrollContainerRef={makeScrollRef()}
      />,
      { wrapper: TestWrapper },
    );

    const bars = screen.getAllByRole("link");

    // Group aggregate bar carries the diagonal-stripe overlay.
    expect(bars[0]?.querySelector('[style*="repeating-linear-gradient"]')).not.toBeNull();
    // Regular task bar does not.
    expect(bars[1]?.querySelector('[style*="repeating-linear-gradient"]')).toBeNull();
  });

  it.each([
    { expected: 2, name: "every task of the group", params: { groupId: "refine" } },
    { expected: 1, name: "only the row itself", params: { taskId: "refine.improve" } },
  ])("lights up $name at the pinned pass", ({ expected, params }) => {
    mockParams = params;
    mockSearchParams = new URLSearchParams("iteration=2");
    const nodes = PASS_MEMBERS.map((id) => ({ ...BASE_NODE, id, label: id }));

    render(
      <GanttTimeline
        {...defaultProps}
        flatNodes={nodes}
        rowSegments={PASS_MEMBERS.map((id) => [passSegment(id, 2)])}
        scrollContainerRef={makeScrollRef()}
      />,
      { wrapper: TestWrapper },
    );

    expect(document.querySelectorAll("[data-pinned-pass]")).toHaveLength(expected);
  });

  it("leaves other passes alone", () => {
    mockParams = { groupId: "refine" };
    mockSearchParams = new URLSearchParams("iteration=2");
    const nodes = PASS_MEMBERS.map((id) => ({ ...BASE_NODE, id, label: id }));

    render(
      <GanttTimeline
        {...defaultProps}
        flatNodes={nodes}
        rowSegments={PASS_MEMBERS.map((id) => [passSegment(id, 5)])}
        scrollContainerRef={makeScrollRef()}
      />,
      { wrapper: TestWrapper },
    );

    expect(document.querySelectorAll("[data-pinned-pass]")).toHaveLength(0);
  });

  it("leaves the highlight open where one run's segments meet", () => {
    mockParams = { taskId: "refine.improve" };
    mockSearchParams = new URLSearchParams("iteration=2");
    // Two segments of the same attempt, as a queued stretch followed by the run itself.
    const shared = { loopIteration: { iteration: 2, loop_id: "refine" }, taskInstanceId: "one-try" };
    const half = MIN_MS + (MAX_MS - MIN_MS) / 2;
    const segments: Array<GanttDataItem> = [
      {
        ...shared,
        state: "queued",
        taskId: "refine.improve",
        tryNumber: 1,
        x: [MIN_MS, half],
        y: "refine.improve",
      },
      {
        ...shared,
        state: "success",
        taskId: "refine.improve",
        tryNumber: 1,
        x: [half, MAX_MS],
        y: "refine.improve",
      },
    ];

    render(
      <GanttTimeline
        {...defaultProps}
        flatNodes={[{ ...BASE_NODE, id: "refine.improve", label: "refine.improve" }]}
        rowSegments={[segments]}
        scrollContainerRef={makeScrollRef()}
      />,
      { wrapper: TestWrapper },
    );

    const [first, second] = [...document.querySelectorAll("[data-pinned-pass]")];
    const styleOf = (node: Element) => globalThis.getComputedStyle(node);

    // The run is ringed at its ends and along its length, but not down the seam between the
    // two states, which is inside the thing being highlighted rather than its edge.
    expect(styleOf(first as Element).borderLeftWidth).toBe("2px");
    expect(styleOf(first as Element).borderRightWidth).not.toBe("2px");
    expect(styleOf(second as Element).borderLeftWidth).not.toBe("2px");
    expect(styleOf(second as Element).borderRightWidth).toBe("2px");
    expect(styleOf(first as Element).borderTopWidth).toBe("2px");
  });

  it("does not read a region index as a pass", () => {
    mockParams = { taskId: "refine.improve" };
    // A mapped expansion's region index is its map slot. Ringing bars because slot 2 happens to
    // look like pass 2 would highlight an unrelated pass.
    mockSearchParams = new URLSearchParams("region_index=2&region_id=expansion");

    render(
      <GanttTimeline
        {...defaultProps}
        flatNodes={[{ ...BASE_NODE, id: "refine.improve", label: "refine.improve" }]}
        rowSegments={[[passSegment("refine.improve", 2)]]}
        scrollContainerRef={makeScrollRef()}
      />,
      { wrapper: TestWrapper },
    );

    expect(document.querySelectorAll("[data-pinned-pass]")).toHaveLength(0);
  });
});
