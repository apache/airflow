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
import { useLayoutEffect, useRef } from "react";

import type { Virtualizer } from "@tanstack/react-virtual";

import { ROW_HEIGHT } from "./constants";
import type { GridTask } from "./utils";

// Each axis is absent until it has actually been scrollable: a Dag whose runs have not arrived
// reports 0 for an axis that cannot move, and storing that 0 would read back as a real position.
type ScrollPosition = {
  left?: number;
  top?: number;
};

// Last Grid scroll offsets per Dag. Module scope so they outlive the Grid remounting.
const gridScrollPositions = new Map<string, ScrollPosition>();

type Params = {
  dagId: string;
  flatNodes: Array<GridTask>;
  // Height of the sticky header above the rows (also the virtualizer's scrollPaddingStart).
  headerPad: number;
  isRtl: boolean;
  // Width kept clear at the inline end for the scrollbar and anything pinned over that edge.
  reservedEndWidth: number;
  rowVirtualizer: Virtualizer<HTMLDivElement, Element>;
  runIds: Array<string>;
  selectedGroupId?: string;
  selectedRunId?: string;
  selectedTaskId?: string;
  // Gantt view shares one scroller with the Grid, where the horizontal axis is the Gantt's timeline.
  sharesScroller: boolean;
  taskNameColumnRef: RefObject<HTMLDivElement | null>;
};

// Horizontal counterpart to the virtualizer's scrollToIndex, with the same meaning of `align`.
//
// Run columns scroll beneath the sticky task name column, so a column tucked behind it is as hidden
// as one past the far edge. The visible scrollport is clientWidth wide (unlike the bounding rect, it
// excludes the vertical scrollbar), minus that column: on the left in LTR, on the right in RTL, where
// the scrollbar also moves to the left. The opposite end gives up its reserved strip for the same
// reason: the scrollbar and the buttons pinned there cover the run columns, and `nearest` would
// otherwise park a run right underneath them. Returns whether the run's column was found.
const scrollRunIntoView = ({
  align,
  isRtl,
  reservedEndWidth,
  scrollEl,
  selectedRunId,
  taskNameColumn,
}: {
  align: "center" | "nearest";
  isRtl: boolean;
  reservedEndWidth: number;
  scrollEl: HTMLElement;
  selectedRunId: string | undefined;
  taskNameColumn: HTMLDivElement | null;
}) => {
  if (selectedRunId === undefined) {
    return false;
  }

  // Both the duration bar and the task instance column carry the run id; either one gives the
  // column's x position, so take whichever the query finds first.
  const column = scrollEl.querySelector(`[data-run-id="${CSS.escape(selectedRunId)}"]`);

  if (!column) {
    return false;
  }

  const columnRect = column.getBoundingClientRect();
  const viewRect = scrollEl.getBoundingClientRect();
  const nameColumnWidth = taskNameColumn?.getBoundingClientRect().width ?? 0;
  const visibleStart = isRtl
    ? viewRect.right - scrollEl.clientWidth + reservedEndWidth
    : viewRect.left + nameColumnWidth;
  const visibleEnd = isRtl
    ? viewRect.right - nameColumnWidth
    : viewRect.left + scrollEl.clientWidth - reservedEndWidth;

  if (columnRect.left >= visibleStart && columnRect.right <= visibleEnd) {
    return true;
  }

  // Nearest corrects whichever edge is cut off; centre puts the column's midpoint on the view's.
  const nearestDelta =
    columnRect.left < visibleStart ? columnRect.left - visibleStart : columnRect.right - visibleEnd;
  const centerDelta = columnRect.left + columnRect.width / 2 - (visibleStart + visibleEnd) / 2;
  const delta = align === "center" ? centerDelta : nearestDelta;

  // A delta in CSS pixels is direction-agnostic: scrollLeft grows rightwards even in RTL, where it
  // runs from -(scrollWidth - clientWidth) to 0. The browser clamps an overshoot at either end, so a
  // run near an edge simply lands as close as it can.
  scrollEl.scrollLeft += delta;

  return true;
};

// A Grid that mounts starts scrolled to the top left (a first visit, or a browser reload), and one
// that stays mounted while the route switches to another Dag still holds the offsets of the Dag it
// was showing. Restoring the offsets saved for this Dag picks up where it was left.
//
// If the content changed since we saved (a new version, groups expanded/collapsed, runs added or
// paged) the offsets no longer line up, so we fall back to revealing whatever is selected: the task
// by index, since fixed-height rows make the index alone give the offset, and the run by measuring
// its column.
export const useGridScrollRestore = ({
  dagId,
  flatNodes,
  headerPad,
  isRtl,
  reservedEndWidth,
  rowVirtualizer,
  runIds,
  selectedGroupId,
  selectedRunId,
  selectedTaskId,
  sharesScroller,
  taskNameColumnRef,
}: Params) => {
  // Value dep: the runs array is rebuilt on every render, so its identity would re-run the effect
  // (and re-measure the DOM) far more often than the columns actually change.
  const runIdsKey = runIds.join("\u0000");

  // What the reveal below last settled on. The run list changes in place (polling brings a new run,
  // the user pages), and re-revealing then would yank the Grid back to the selection after the user
  // scrolled away from it. Starting empty, a freshly mounted Grid reveals whatever is selected.
  const revealed = useRef<{ anchorId?: string; cell?: string; flatNodes?: Array<GridTask> }>({});

  useLayoutEffect(() => {
    const scrollEl = rowVirtualizer.scrollElement;

    if (!scrollEl) {
      return undefined;
    }

    const saved = gridScrollPositions.get(dagId);

    if (saved?.top !== undefined) {
      scrollEl.scrollTop = saved.top;
    }

    if (saved?.left !== undefined && !sharesScroller) {
      scrollEl.scrollLeft = saved.left;
    }

    const anchorId = selectedTaskId ?? selectedGroupId;
    const index = flatNodes.findIndex((node) => node.id === anchorId);

    // Rows only move when the structure does (a new version, groups toggled), so the task is
    // re-revealed then or when the selection changes, never because the runs did.
    if (
      index !== -1 &&
      (revealed.current.anchorId !== anchorId || revealed.current.flatNodes !== flatNodes)
    ) {
      const rowTop = headerPad + index * ROW_HEIGHT;
      const outOfView =
        rowTop < scrollEl.scrollTop || rowTop + ROW_HEIGHT > scrollEl.scrollTop + scrollEl.clientHeight;

      if (outOfView) {
        rowVirtualizer.scrollToIndex(index, { align: "center" });
      }
      revealed.current = { ...revealed.current, anchorId, flatNodes };
    }

    // Sharing a scroller means moving it horizontally would drag the Gantt's timeline along with
    // the run columns, so the Grid leaves that axis to the Gantt. The run is revealed once per
    // selected cell, as soon as its column has rendered: runs arrive after the first paint, which is
    // why the run ids are a dependency at all. Picking another task in the same run (from the details
    // panel, while the Grid is scrolled away) is a new selection, so it reveals the run too.
    const cell = `${selectedRunId ?? ""}\u0000${anchorId ?? ""}`;

    if (!sharesScroller && revealed.current.cell !== cell) {
      const found = scrollRunIntoView({
        // Centre on a first visit, where the run lands somewhere readable rather than flush against
        // an edge; once there is a position worth keeping, move as little as possible. A saved
        // vertical offset says nothing about the runs, so only a horizontal one counts as one.
        align: saved?.left === undefined ? "center" : "nearest",
        isRtl,
        reservedEndWidth,
        scrollEl,
        selectedRunId,
        taskNameColumn: taskNameColumnRef.current,
      });

      if (found || selectedRunId === undefined) {
        revealed.current = { ...revealed.current, cell };
      }
    }

    const handleScroll = () => {
      const previous = gridScrollPositions.get(dagId);
      const canScrollX = scrollEl.scrollWidth > scrollEl.clientWidth;
      const canScrollY = scrollEl.scrollHeight > scrollEl.clientHeight;

      gridScrollPositions.set(dagId, {
        left: canScrollX ? scrollEl.scrollLeft : previous?.left,
        top: canScrollY ? scrollEl.scrollTop : previous?.top,
      });
    };

    scrollEl.addEventListener("scroll", handleScroll, { passive: true });

    return () => scrollEl.removeEventListener("scroll", handleScroll);
  }, [
    dagId,
    flatNodes,
    headerPad,
    isRtl,
    reservedEndWidth,
    rowVirtualizer,
    runIdsKey,
    selectedGroupId,
    selectedRunId,
    selectedTaskId,
    sharesScroller,
    taskNameColumnRef,
  ]);
};
