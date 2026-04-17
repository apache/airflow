/* eslint-disable max-lines */

/* eslint-disable unicorn/no-null */

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
import { act, render, screen } from "@testing-library/react";
import type { PropsWithChildren, RefObject } from "react";
import { createRef } from "react";
import { afterEach, describe, expect, it, vi } from "vitest";

import { HoverProvider } from "src/context/hover";
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

const TestWrapper = ({ children }: PropsWithChildren) => (
  <Wrapper>
    <HoverProvider>{children}</HoverProvider>
  </Wrapper>
);

// Shared time range: 10:00 → 10:10 UTC on 2024-03-14
const MIN_MS = new Date("2024-03-14T10:00:00Z").getTime();
const MAX_MS = new Date("2024-03-14T10:10:00Z").getTime();

const BASE_NODE: GridTask = {
  depth: 0,
  id: "task_1",
  is_mapped: false,
  label: "task_1",
} as GridTask;

const makeScrollRef = (): RefObject<HTMLDivElement | null> => {
  const ref = createRef<HTMLDivElement | null>();

  (ref as { current: HTMLDivElement | null }).current = document.createElement("div");

  return ref;
};

const defaultProps = {
  dagId: "test_dag",
  flatNodes: [BASE_NODE],
  ganttDataItems: [] as Array<GanttDataItem>,
  gridSummaries: [],
  maxMs: MAX_MS,
  minMs: MIN_MS,
  rowSegments: [[]] as Array<Array<GanttDataItem>>,
  runId: "run_1",
  virtualizerScrollPaddingStart: ROW_HEIGHT,
};

describe("GanttTimeline segment bars", () => {
  afterEach(() => {
    vi.unstubAllGlobals();
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
        ganttDataItems={[executionSegment]}
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
      <GanttTimeline
        {...defaultProps}
        ganttDataItems={segments}
        rowSegments={[segments]}
        scrollContainerRef={makeScrollRef()}
      />,
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
      <GanttTimeline
        {...defaultProps}
        ganttDataItems={segments}
        rowSegments={[segments]}
        scrollContainerRef={makeScrollRef()}
      />,
      { wrapper: TestWrapper },
    );

    expect(screen.getAllByRole("link")).toHaveLength(3);
  });

  it("renders bars for multiple tasks independently", () => {
    const node2: GridTask = { depth: 0, id: "task_2", is_mapped: false, label: "task_2" } as GridTask;

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
        ganttDataItems={[t1Scheduled, t1Exec, t2Exec]}
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
        ganttDataItems={[]}
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
      <GanttTimeline
        {...defaultProps}
        ganttDataItems={segments}
        rowSegments={[segments]}
        scrollContainerRef={makeScrollRef()}
      />,
      { wrapper: TestWrapper },
    );

    // Fire the ResizeObserver callback to set bodyWidthPx = 1000, enabling the filter.
    act(() => {
      observerCallback?.([{ contentRect: { width: 1000 } } as ResizeObserverEntry], {} as ResizeObserver);
    });

    // Only the execution bar should be rendered; scheduled and queued are too narrow.
    expect(screen.getAllByRole("link")).toHaveLength(1);
  });
});
