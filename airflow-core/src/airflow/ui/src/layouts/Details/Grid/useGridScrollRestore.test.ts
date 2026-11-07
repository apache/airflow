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
import type { RefObject } from "react";

import type { Virtualizer } from "@tanstack/react-virtual";
import { renderHook } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";

import { ROW_HEIGHT } from "./constants";
import { useGridScrollRestore } from "./useGridScrollRestore";
import type { GridTask } from "./utils";

const HEADER_PAD = 50;
const TASK_NAME_COLUMN_WIDTH = 200;
const VIEWPORT_WIDTH = 500;
const RUN_COLUMN_WIDTH = 18;

const clampToRange = (value: number, extent: number) => Math.max(0, Math.min(value, extent));

/**
 * Fake scroll container exposing only what the hook touches, plus a way to fire a scroll event.
 * ``runColumnLeft`` places the selected run column at that offset within the scrolled content; its
 * rect then tracks ``scrollLeft`` the way a real column does.
 */
const makeScrollEl = ({
  clientHeight = 100,
  rtl = false,
  runColumnLeft,
  scrollHeight = 10_000,
  scrollWidth = 10_000,
}: {
  clientHeight?: number;
  rtl?: boolean;
  runColumnLeft?: number;
  scrollHeight?: number;
  scrollWidth?: number;
} = {}) => {
  const handlers: Array<() => void> = [];
  const scrollEl = {
    addEventListener: vi.fn((_type: string, cb: () => void) => handlers.push(cb)),
    clientHeight,
    clientWidth: VIEWPORT_WIDTH,
    dispatchScroll: () => handlers.forEach((cb) => cb()),
    getBoundingClientRect: () => ({ left: 0, right: VIEWPORT_WIDTH }) as DOMRect,
    querySelector: vi.fn((selector: string) => {
      if (runColumnLeft === undefined || !selector.includes("data-run-id")) {
        return null;
      }

      return {
        getBoundingClientRect: () => {
          const left = runColumnLeft - scrollEl.scrollLeft;

          return { left, right: left + RUN_COLUMN_WIDTH, width: RUN_COLUMN_WIDTH } as DOMRect;
        },
      };
    }),
    removeEventListener: vi.fn((_type: string, cb: () => void) => {
      const index = handlers.indexOf(cb);

      if (index >= 0) {
        handlers.splice(index, 1);
      }
    }),
    scrollHeight,
    scrollLeft: 0,
    scrollTop: 0,
    scrollWidth,
  };

  // A real container clamps a programmatic offset to its scrollable range, which is what keeps a
  // centred run near either end from overshooting. In RTL that range is negative, ending at 0.
  let currentLeft = 0;
  let currentTop = 0;

  Object.defineProperties(scrollEl, {
    scrollLeft: {
      get: () => currentLeft,
      set: (value: number) => {
        const extent = scrollEl.scrollWidth - scrollEl.clientWidth;

        currentLeft = rtl ? -clampToRange(-value, extent) : clampToRange(value, extent);
      },
    },
    scrollTop: {
      get: () => currentTop,
      set: (value: number) => {
        currentTop = clampToRange(value, scrollEl.scrollHeight - scrollEl.clientHeight);
      },
    },
  });

  return scrollEl;
};

/** Where centring puts a column that starts at `contentLeft`, given the fake's fixed geometry. */
const centeredScrollLeft = (contentLeft: number) =>
  contentLeft + RUN_COLUMN_WIDTH / 2 - (TASK_NAME_COLUMN_WIDTH + VIEWPORT_WIDTH) / 2;

const makeVirtualizer = (scrollElement: ReturnType<typeof makeScrollEl> | null) => {
  const scrollToIndex = vi.fn();

  return {
    rowVirtualizer: { scrollElement, scrollToIndex } as unknown as Virtualizer<HTMLDivElement, Element>,
    scrollToIndex,
  };
};

const taskNameColumnRef = {
  current: { getBoundingClientRect: () => ({ width: TASK_NAME_COLUMN_WIDTH }) },
} as unknown as RefObject<HTMLDivElement | null>;

const makeTask = (id: string): GridTask => ({ id }) as unknown as GridTask;
const makeTasks = (count: number) => Array.from({ length: count }, (_, index) => makeTask(`task_${index}`));

type HookArgs = Parameters<typeof useGridScrollRestore>[0];
type Defaulted = "headerPad" | "isRtl" | "sharesScroller" | "taskNameColumnRef";
type RestoreArgs = Omit<HookArgs, Defaulted> & Partial<Pick<HookArgs, Defaulted>>;

const renderRestore = (args: RestoreArgs) =>
  renderHook(
    (props: RestoreArgs) =>
      useGridScrollRestore({
        headerPad: HEADER_PAD,
        isRtl: false,
        sharesScroller: false,
        taskNameColumnRef,
        ...props,
      }),
    { initialProps: args },
  );

describe("useGridScrollRestore", () => {
  it("does nothing when there is no scroll element yet", () => {
    const { rowVirtualizer, scrollToIndex } = makeVirtualizer(null);

    renderRestore({
      dagId: "dag_no_el",
      flatNodes: makeTasks(3),
      rowVirtualizer,
      runIds: [],
      selectedTaskId: "task_2",
    });

    expect(scrollToIndex).not.toHaveBeenCalled();
  });

  it("leaves the scroll position untouched on the first visit to a Dag", () => {
    const scrollEl = makeScrollEl();
    const { rowVirtualizer } = makeVirtualizer(scrollEl);

    renderRestore({ dagId: "dag_unseen", flatNodes: [], rowVirtualizer, runIds: [] });

    expect(scrollEl.scrollTop).toBe(0);
    expect(scrollEl.scrollLeft).toBe(0);
  });

  it("saves both offsets on scroll and restores them after a remount", () => {
    const first = makeScrollEl();
    const before = makeVirtualizer(first);
    const { unmount } = renderRestore({
      dagId: "dag_restore",
      flatNodes: [],
      rowVirtualizer: before.rowVirtualizer,
      runIds: [],
    });

    first.scrollTop = 240;
    first.scrollLeft = 360;
    first.dispatchScroll();
    unmount();

    const second = makeScrollEl();
    const after = makeVirtualizer(second);

    renderRestore({
      dagId: "dag_restore",
      flatNodes: [],
      rowVirtualizer: after.rowVirtualizer,
      runIds: [],
    });

    expect(second.scrollTop).toBe(240);
    expect(second.scrollLeft).toBe(360);
  });

  it("keeps the saved offsets when an axis cannot scroll yet", () => {
    const scrollEl = makeScrollEl();
    const { rowVirtualizer } = makeVirtualizer(scrollEl);
    const { unmount } = renderRestore({
      dagId: "dag_unscrollable",
      flatNodes: [],
      rowVirtualizer,
      runIds: [],
    });

    scrollEl.scrollLeft = 180;
    scrollEl.dispatchScroll();

    // The runs have gone away, so the browser clamps the offset to 0 and fires a scroll event.
    scrollEl.scrollWidth = VIEWPORT_WIDTH;
    scrollEl.scrollLeft = 0;
    scrollEl.dispatchScroll();
    unmount();

    const remounted = makeScrollEl();
    const after = makeVirtualizer(remounted);

    renderRestore({
      dagId: "dag_unscrollable",
      flatNodes: [],
      rowVirtualizer: after.rowVirtualizer,
      runIds: [],
    });

    expect(remounted.scrollLeft).toBe(180);
  });

  it("centers the selected task when the structure changed and it is out of view", () => {
    const scrollEl = makeScrollEl({ clientHeight: HEADER_PAD + 2 * ROW_HEIGHT });
    const { rowVirtualizer, scrollToIndex } = makeVirtualizer(scrollEl);

    renderRestore({
      dagId: "dag_center",
      flatNodes: makeTasks(20),
      rowVirtualizer,
      runIds: [],
      selectedTaskId: "task_10",
    });

    expect(scrollToIndex).toHaveBeenCalledWith(10, { align: "center" });
  });

  it("centers a selected group when no task is selected", () => {
    const scrollEl = makeScrollEl({ clientHeight: HEADER_PAD + 2 * ROW_HEIGHT });
    const flatNodes = [...makeTasks(15), makeTask("group_x")];
    const { rowVirtualizer, scrollToIndex } = makeVirtualizer(scrollEl);

    renderRestore({
      dagId: "dag_group",
      flatNodes,
      rowVirtualizer,
      runIds: [],
      selectedGroupId: "group_x",
    });

    expect(scrollToIndex).toHaveBeenCalledWith(15, { align: "center" });
  });

  it("does not scroll when the selected task is already visible", () => {
    const scrollEl = makeScrollEl({ clientHeight: HEADER_PAD + 4 * ROW_HEIGHT });
    const { rowVirtualizer, scrollToIndex } = makeVirtualizer(scrollEl);

    renderRestore({
      dagId: "dag_visible",
      flatNodes: makeTasks(20),
      rowVirtualizer,
      runIds: [],
      selectedTaskId: "task_1",
    });

    expect(scrollToIndex).not.toHaveBeenCalled();
  });

  it("does nothing when the selected id is not among the rows", () => {
    const scrollEl = makeScrollEl({ clientHeight: HEADER_PAD + 2 * ROW_HEIGHT });
    const { rowVirtualizer, scrollToIndex } = makeVirtualizer(scrollEl);

    renderRestore({
      dagId: "dag_missing",
      flatNodes: makeTasks(20),
      rowVirtualizer,
      runIds: [],
      selectedTaskId: "task_missing",
    });

    expect(scrollToIndex).not.toHaveBeenCalled();
  });

  it("centers the selected run when it sits past the right edge on a fresh visit", () => {
    const scrollEl = makeScrollEl({ runColumnLeft: 800 });
    const { rowVirtualizer } = makeVirtualizer(scrollEl);

    renderRestore({
      dagId: "dag_run_right",
      flatNodes: [],
      rowVirtualizer,
      runIds: ["run_1"],
      selectedRunId: "run_1",
    });

    expect(scrollEl.scrollLeft).toBe(centeredScrollLeft(800));
  });

  it("counts a run behind the sticky task name column as hidden", () => {
    const scrollEl = makeScrollEl({ runColumnLeft: 600 });
    const { rowVirtualizer } = makeVirtualizer(scrollEl);

    // Left of the scrollport but behind the task name column, so a bare left >= 0 check would call
    // it visible and leave it there.
    scrollEl.scrollLeft = 550;

    renderRestore({
      dagId: "dag_run_behind_names",
      flatNodes: [],
      rowVirtualizer,
      runIds: ["run_1"],
      selectedRunId: "run_1",
    });

    expect(scrollEl.scrollLeft).toBe(centeredScrollLeft(600));
  });

  it("nudges rather than centers the selected run once a position has been saved", () => {
    const first = makeScrollEl({ runColumnLeft: 800 });
    const before = makeVirtualizer(first);
    const { unmount } = renderRestore({
      dagId: "dag_run_nudge",
      flatNodes: [],
      rowVirtualizer: before.rowVirtualizer,
      runIds: ["run_1"],
      selectedRunId: "run_1",
    });

    first.dispatchScroll();
    unmount();

    // Remounted with the run drifted left of the restored offset, as new runs push the window along.
    const second = makeScrollEl({ runColumnLeft: 490 });
    const after = makeVirtualizer(second);

    renderRestore({
      dagId: "dag_run_nudge",
      flatNodes: [],
      rowVirtualizer: after.rowVirtualizer,
      runIds: ["run_1", "run_2"],
      selectedRunId: "run_1",
    });

    expect(second.scrollLeft).toBe(490 - TASK_NAME_COLUMN_WIDTH);
    expect(second.scrollLeft).not.toBe(centeredScrollLeft(490));
  });

  it("leaves the scroll position alone when the selected run is already visible", () => {
    const scrollEl = makeScrollEl({ runColumnLeft: 300 });
    const { rowVirtualizer } = makeVirtualizer(scrollEl);

    renderRestore({
      dagId: "dag_run_visible",
      flatNodes: [],
      rowVirtualizer,
      runIds: ["run_1"],
      selectedRunId: "run_1",
    });

    expect(scrollEl.scrollLeft).toBe(0);
  });

  it("does not scroll horizontally when the selected run has no column", () => {
    const scrollEl = makeScrollEl();
    const { rowVirtualizer } = makeVirtualizer(scrollEl);

    renderRestore({
      dagId: "dag_run_absent",
      flatNodes: [],
      rowVirtualizer,
      runIds: ["run_1"],
      selectedRunId: "run_1",
    });

    expect(scrollEl.scrollLeft).toBe(0);
  });

  it("centers the selected run when only a vertical position was saved", () => {
    // A deep link to a task scrolls the Grid down before the runs have arrived. That scroll must
    // not count as a horizontal position, or the run is merely nudged to the edge afterwards.
    const beforeRuns = makeScrollEl({ runColumnLeft: 800, scrollWidth: VIEWPORT_WIDTH });
    const before = makeVirtualizer(beforeRuns);
    const { unmount } = renderRestore({
      dagId: "dag_vertical_only",
      flatNodes: [],
      rowVirtualizer: before.rowVirtualizer,
      runIds: [],
      selectedRunId: "run_1",
    });

    beforeRuns.scrollTop = 400;
    beforeRuns.dispatchScroll();
    unmount();

    const withRuns = makeScrollEl({ runColumnLeft: 800 });
    const after = makeVirtualizer(withRuns);

    renderRestore({
      dagId: "dag_vertical_only",
      flatNodes: [],
      rowVirtualizer: after.rowVirtualizer,
      runIds: ["run_1"],
      selectedRunId: "run_1",
    });

    expect(withRuns.scrollLeft).toBe(centeredScrollLeft(800));
    expect(withRuns.scrollTop).toBe(400);
  });

  it("leaves the horizontal axis alone when the scroller is shared with the Gantt", () => {
    const scrollEl = makeScrollEl({ runColumnLeft: 800 });
    const { rowVirtualizer } = makeVirtualizer(scrollEl);

    renderRestore({
      dagId: "dag_shared_scroller",
      flatNodes: [],
      rowVirtualizer,
      runIds: ["run_1"],
      selectedRunId: "run_1",
      sharesScroller: true,
    });

    expect(scrollEl.scrollLeft).toBe(0);
  });

  it("does not move the Grid when a refresh brings the same runs", () => {
    const scrollEl = makeScrollEl({ runColumnLeft: 800 });
    const virtualizer = makeVirtualizer(scrollEl);
    const args = {
      dagId: "dag_same_runs",
      flatNodes: [],
      rowVirtualizer: virtualizer.rowVirtualizer,
      runIds: ["run_1", "run_2"],
      selectedRunId: "run_1",
    };
    const rendered = renderRestore(args);

    scrollEl.dispatchScroll();
    scrollEl.scrollLeft = 120;
    scrollEl.dispatchScroll();

    // The poll rebuilds the array every render; only the ids in it should matter.
    rendered.rerender({ ...args, runIds: ["run_1", "run_2"] });

    expect(scrollEl.scrollLeft).toBe(120);
  });

  it("does not pull the Grid back to the selection when the run list changes", () => {
    const scrollEl = makeScrollEl({ runColumnLeft: 800 });
    const { rowVirtualizer, scrollToIndex } = makeVirtualizer(scrollEl);
    const args = {
      dagId: "dag_new_runs",
      flatNodes: makeTasks(50),
      rowVirtualizer,
      runIds: ["run_1", "run_2"],
      selectedRunId: "run_1",
      selectedTaskId: "task_40",
    };
    const rendered = renderRestore(args);

    expect(scrollToIndex).toHaveBeenCalledTimes(1);

    // The user scrolls away from the selected cell on both axes.
    scrollEl.scrollLeft = 0;
    scrollEl.scrollTop = 0;
    scrollEl.dispatchScroll();

    // A poll brings a new run, then the user pages to older ones.
    rendered.rerender({ ...args, runIds: ["run_0", "run_1", "run_2"] });
    rendered.rerender({ ...args, runIds: ["run_3", "run_4"] });

    expect(scrollEl.scrollLeft).toBe(0);
    expect(scrollToIndex).toHaveBeenCalledTimes(1);

    // Selecting something else is a reason to reveal again.
    rendered.rerender({
      ...args,
      runIds: ["run_3", "run_4"],
      selectedRunId: "run_4",
      selectedTaskId: "task_45",
    });

    expect(scrollToIndex).toHaveBeenCalledTimes(2);
    // Nudged rather than centred, since the user's scroll saved a horizontal position.
    expect(scrollEl.scrollLeft).toBe(800 + RUN_COLUMN_WIDTH - VIEWPORT_WIDTH);
  });

  it("reveals the run again when another task in the same run is selected", () => {
    const scrollEl = makeScrollEl({ runColumnLeft: 800 });
    const { rowVirtualizer } = makeVirtualizer(scrollEl);
    const args = {
      dagId: "dag_same_run_other_task",
      flatNodes: makeTasks(5),
      rowVirtualizer,
      runIds: ["run_1"],
      selectedRunId: "run_1",
      selectedTaskId: "task_0",
    };
    const rendered = renderRestore(args);

    scrollEl.scrollLeft = 0;
    scrollEl.dispatchScroll();

    // Picked from the details panel while the run's column is scrolled out of view.
    rendered.rerender({ ...args, selectedTaskId: "task_1" });

    expect(scrollEl.scrollLeft).toBe(800 + RUN_COLUMN_WIDTH - VIEWPORT_WIDTH);
  });

  it("reveals the selected run once its column renders after the first paint", () => {
    const scrollEl = makeScrollEl({ runColumnLeft: 800 });
    const { rowVirtualizer } = makeVirtualizer(scrollEl);
    const findColumn = scrollEl.querySelector.getMockImplementation();

    // The runs query has not answered yet, so the column is not in the DOM.
    scrollEl.querySelector.mockImplementation(() => null);
    const args = {
      dagId: "dag_runs_late",
      flatNodes: [],
      rowVirtualizer,
      runIds: [],
      selectedRunId: "run_1",
    };
    const rendered = renderRestore(args);

    expect(scrollEl.scrollLeft).toBe(0);

    if (findColumn) {
      scrollEl.querySelector.mockImplementation(findColumn);
    }
    rendered.rerender({ ...args, runIds: ["run_1"] });

    expect(scrollEl.scrollLeft).toBe(centeredScrollLeft(800));
  });

  it("in RTL, counts a run behind the task name column on the right as hidden", () => {
    // RTL pins the task name column to the right edge, so the runs are visible on [0, 300].
    const scrollEl = makeScrollEl({ rtl: true, runColumnLeft: -50 });
    const { rowVirtualizer } = makeVirtualizer(scrollEl);

    // Scrolled so the run sits at [350, 368]: inside the scrollport, but under the names.
    scrollEl.scrollLeft = -400;

    renderRestore({
      dagId: "dag_rtl_behind_names",
      flatNodes: [],
      isRtl: true,
      rowVirtualizer,
      runIds: ["run_1"],
      selectedRunId: "run_1",
    });

    // Centred on the visible part: the column's midpoint lands on 150.
    const columnLeft = -50 - scrollEl.scrollLeft;

    expect(columnLeft + RUN_COLUMN_WIDTH / 2).toBe((VIEWPORT_WIDTH - TASK_NAME_COLUMN_WIDTH) / 2);
  });

  it("keeps each Dag's position to itself", () => {
    const dagA = makeScrollEl({ runColumnLeft: 800 });
    const firstA = makeVirtualizer(dagA);
    const visitA = renderRestore({
      dagId: "dag_a",
      flatNodes: [],
      rowVirtualizer: firstA.rowVirtualizer,
      runIds: ["run_1"],
    });

    dagA.scrollLeft = 300;
    dagA.dispatchScroll();
    visitA.unmount();

    const dagB = makeScrollEl({ runColumnLeft: 800 });
    const firstB = makeVirtualizer(dagB);
    const visitB = renderRestore({
      dagId: "dag_b",
      flatNodes: [],
      rowVirtualizer: firstB.rowVirtualizer,
      runIds: ["run_1"],
      selectedRunId: "run_1",
    });

    expect(dagB.scrollLeft).toBe(centeredScrollLeft(800));
    visitB.unmount();

    const backToA = makeScrollEl({ runColumnLeft: 800 });
    const secondA = makeVirtualizer(backToA);

    renderRestore({
      dagId: "dag_a",
      flatNodes: [],
      rowVirtualizer: secondA.rowVirtualizer,
      runIds: ["run_1"],
    });

    expect(backToA.scrollLeft).toBe(300);
  });

  it("removes the scroll listener on unmount", () => {
    const scrollEl = makeScrollEl();
    const { rowVirtualizer } = makeVirtualizer(scrollEl);
    const { unmount } = renderRestore({
      dagId: "dag_cleanup",
      flatNodes: [],
      rowVirtualizer,
      runIds: [],
    });

    expect(scrollEl.addEventListener).toHaveBeenCalledWith("scroll", expect.any(Function), { passive: true });

    unmount();

    expect(scrollEl.removeEventListener).toHaveBeenCalledWith("scroll", expect.any(Function));
  });
});
