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
import type { Virtualizer } from "@tanstack/react-virtual";
import { renderHook } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";

import { ROW_HEIGHT } from "./constants";
import { useGridScrollRestore } from "./useGridScrollRestore";
import type { GridTask } from "./utils";

const HEADER_PAD = 50;

/** Fake scroll container exposing only what the hook touches, plus a way to fire a scroll event. */
const makeScrollEl = (clientHeight: number) => {
  const handlers: Array<() => void> = [];

  return {
    addEventListener: vi.fn((_type: string, cb: () => void) => handlers.push(cb)),
    clientHeight,
    dispatchScroll: () => handlers.forEach((cb) => cb()),
    removeEventListener: vi.fn((_type: string, cb: () => void) => {
      const index = handlers.indexOf(cb);

      if (index >= 0) {
        handlers.splice(index, 1);
      }
    }),
    scrollTop: 0,
  };
};

const makeVirtualizer = (scrollElement: ReturnType<typeof makeScrollEl> | null) => {
  const scrollToIndex = vi.fn();

  return {
    rowVirtualizer: { scrollElement, scrollToIndex } as unknown as Virtualizer<HTMLDivElement, Element>,
    scrollToIndex,
  };
};

const makeTask = (id: string): GridTask => ({ id }) as unknown as GridTask;
const makeTasks = (count: number) => Array.from({ length: count }, (_, index) => makeTask(`task_${index}`));

describe("useGridScrollRestore", () => {
  it("does nothing when there is no scroll element yet", () => {
    const { rowVirtualizer, scrollToIndex } = makeVirtualizer(null);

    renderHook(() =>
      useGridScrollRestore({
        dagId: "dag_no_el",
        flatNodes: makeTasks(3),
        headerPad: HEADER_PAD,
        rowVirtualizer,
        selectedTaskId: "task_2",
      }),
    );

    expect(scrollToIndex).not.toHaveBeenCalled();
  });

  it("leaves the scroll position untouched on the first visit to a Dag", () => {
    const scrollEl = makeScrollEl(100);
    const { rowVirtualizer } = makeVirtualizer(scrollEl);

    renderHook(() =>
      useGridScrollRestore({ dagId: "dag_unseen", flatNodes: [], headerPad: HEADER_PAD, rowVirtualizer }),
    );

    expect(scrollEl.scrollTop).toBe(0);
  });

  it("saves the position on scroll and restores it after a remount", () => {
    const first = makeScrollEl(100);
    const before = makeVirtualizer(first);
    const { unmount } = renderHook(() =>
      useGridScrollRestore({
        dagId: "dag_restore",
        flatNodes: [],
        headerPad: HEADER_PAD,
        rowVirtualizer: before.rowVirtualizer,
      }),
    );

    first.scrollTop = 240;
    first.dispatchScroll();
    unmount();

    const second = makeScrollEl(100);
    const after = makeVirtualizer(second);

    renderHook(() =>
      useGridScrollRestore({
        dagId: "dag_restore",
        flatNodes: [],
        headerPad: HEADER_PAD,
        rowVirtualizer: after.rowVirtualizer,
      }),
    );

    expect(second.scrollTop).toBe(240);
  });

  it("centers the selected task when the structure changed and it is out of view", () => {
    const scrollEl = makeScrollEl(HEADER_PAD + 2 * ROW_HEIGHT);
    const { rowVirtualizer, scrollToIndex } = makeVirtualizer(scrollEl);

    renderHook(() =>
      useGridScrollRestore({
        dagId: "dag_center",
        flatNodes: makeTasks(20),
        headerPad: HEADER_PAD,
        rowVirtualizer,
        selectedTaskId: "task_10",
      }),
    );

    expect(scrollToIndex).toHaveBeenCalledWith(10, { align: "center" });
  });

  it("centers a selected group when no task is selected", () => {
    const scrollEl = makeScrollEl(HEADER_PAD + 2 * ROW_HEIGHT);
    const flatNodes = [...makeTasks(15), makeTask("group_x")];
    const { rowVirtualizer, scrollToIndex } = makeVirtualizer(scrollEl);

    renderHook(() =>
      useGridScrollRestore({
        dagId: "dag_group",
        flatNodes,
        headerPad: HEADER_PAD,
        rowVirtualizer,
        selectedGroupId: "group_x",
      }),
    );

    expect(scrollToIndex).toHaveBeenCalledWith(15, { align: "center" });
  });

  it("does not scroll when the selected task is already visible", () => {
    const scrollEl = makeScrollEl(HEADER_PAD + 4 * ROW_HEIGHT);
    const { rowVirtualizer, scrollToIndex } = makeVirtualizer(scrollEl);

    renderHook(() =>
      useGridScrollRestore({
        dagId: "dag_visible",
        flatNodes: makeTasks(20),
        headerPad: HEADER_PAD,
        rowVirtualizer,
        selectedTaskId: "task_1",
      }),
    );

    expect(scrollToIndex).not.toHaveBeenCalled();
  });

  it("does nothing when the selected id is not among the rows", () => {
    const scrollEl = makeScrollEl(HEADER_PAD + 2 * ROW_HEIGHT);
    const { rowVirtualizer, scrollToIndex } = makeVirtualizer(scrollEl);

    renderHook(() =>
      useGridScrollRestore({
        dagId: "dag_missing",
        flatNodes: makeTasks(20),
        headerPad: HEADER_PAD,
        rowVirtualizer,
        selectedTaskId: "task_missing",
      }),
    );

    expect(scrollToIndex).not.toHaveBeenCalled();
  });

  it("removes the scroll listener on unmount", () => {
    const scrollEl = makeScrollEl(100);
    const { rowVirtualizer } = makeVirtualizer(scrollEl);
    const { unmount } = renderHook(() =>
      useGridScrollRestore({ dagId: "dag_cleanup", flatNodes: [], headerPad: HEADER_PAD, rowVirtualizer }),
    );

    expect(scrollEl.addEventListener).toHaveBeenCalledWith("scroll", expect.any(Function), { passive: true });

    unmount();

    expect(scrollEl.removeEventListener).toHaveBeenCalledWith("scroll", expect.any(Function));
  });
});
