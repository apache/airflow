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
import { useLayoutEffect } from "react";

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
  rowVirtualizer: Virtualizer<HTMLDivElement, Element>;
  runIds: Array<string>;
  selectedGroupId?: string;
  selectedRunId?: string;
  selectedTaskId?: string;
  // Gantt view shares one scroller with the Grid, where the horizontal axis is the Gantt's timeline.
  sharesScroller: boolean;
  taskNameColumnRef: RefObject<HTMLDivElement | null>;
};

// Run columns scroll horizontally beneath the sticky task name column, so a column tucked behind it
// is as hidden as one past the right edge: the visible scrollport starts where that column ends and
// ends at clientWidth (which, unlike the bounding rect, excludes the vertical scrollbar).
//
// `center` separates the two ways a run ends up off screen. Arriving fresh (a page load, a new Dag)
// there is no position worth preserving, so the run is centered and lands somewhere readable rather
// than flush against an edge. Once a position exists, the run only drifts out of view a column at a
// time as new runs push the window along, and the smallest correction keeps a Grid the user is
// watching from jumping.
const scrollSelectedRunIntoView = ({
  center,
  scrollEl,
  selectedRunId,
  taskNameColumn,
}: {
  center: boolean;
  scrollEl: HTMLElement;
  selectedRunId: string | undefined;
  taskNameColumn: HTMLDivElement | null;
}) => {
  if (selectedRunId === undefined) {
    return;
  }

  // Both the duration bar and the task instance column carry the run id; either one gives the
  // column's x position, so take whichever the query finds first.
  const column = scrollEl.querySelector(`[data-run-id="${CSS.escape(selectedRunId)}"]`);

  if (!column) {
    return;
  }

  const columnRect = column.getBoundingClientRect();
  const viewRect = scrollEl.getBoundingClientRect();
  const visibleStart = viewRect.left + (taskNameColumn?.getBoundingClientRect().width ?? 0);
  const visibleEnd = viewRect.left + scrollEl.clientWidth;

  if (columnRect.left >= visibleStart && columnRect.right <= visibleEnd) {
    return;
  }

  // Both branches shift by a delta in CSS pixels, which is direction-agnostic: scrollLeft grows
  // rightwards even in RTL, where it runs from -(scrollWidth - clientWidth) to 0. The browser
  // clamps an overshoot at either end, so a run near an edge simply lands as close as it can.
  if (center) {
    const centerOfView = (visibleStart + visibleEnd) / 2;

    scrollEl.scrollLeft += columnRect.left + columnRect.width / 2 - centerOfView;
  } else if (columnRect.left < visibleStart) {
    scrollEl.scrollLeft += columnRect.left - visibleStart;
  } else {
    scrollEl.scrollLeft += columnRect.right - visibleEnd;
  }
};

// Navigating into a run or task swaps the route (Dag/Run/TaskInstance each render their own Grid),
// so the Grid unmounts and a fresh one mounts scrolled back to the top left. We restore the last
// position here so a click doesn't jump the Grid: the rows and run columns are unchanged across the
// remount, so the saved pixel offsets still point at the same cells.
//
// If the content did change since we saved (a new version, groups expanded/collapsed, runs added or
// paged) the saved offsets no longer line up, so we fall back to locating what is selected and
// bringing it into view: the task by index (rows are a fixed height, so the index alone gives the
// offset — no dependency on the stored pixels) and the run column by its measured position. With no
// saved offsets at all — a page load straight onto a run or task instance — both are centered.
export const useGridScrollRestore = ({
  dagId,
  flatNodes,
  headerPad,
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

    if (index !== -1) {
      const rowTop = headerPad + index * ROW_HEIGHT;
      const outOfView =
        rowTop < scrollEl.scrollTop || rowTop + ROW_HEIGHT > scrollEl.scrollTop + scrollEl.clientHeight;

      if (outOfView) {
        rowVirtualizer.scrollToIndex(index, { align: "center" });
      }
    }

    // Sharing a scroller means moving it horizontally would drag the Gantt's timeline along with
    // the run columns, so the Grid leaves that axis to the Gantt.
    if (!sharesScroller) {
      scrollSelectedRunIntoView({
        // A saved vertical offset says nothing about where the runs were; only a saved horizontal
        // one means the user has a position worth keeping instead of being centred.
        center: saved?.left === undefined,
        scrollEl,
        selectedRunId,
        taskNameColumn: taskNameColumnRef.current,
      });
    }

    const handleScroll = () => {
      const previous = gridScrollPositions.get(dagId);

      gridScrollPositions.set(dagId, {
        left: scrollEl.scrollWidth > scrollEl.clientWidth ? scrollEl.scrollLeft : previous?.left,
        top: scrollEl.scrollHeight > scrollEl.clientHeight ? scrollEl.scrollTop : previous?.top,
      });
    };

    scrollEl.addEventListener("scroll", handleScroll, { passive: true });

    return () => scrollEl.removeEventListener("scroll", handleScroll);
  }, [
    dagId,
    flatNodes,
    headerPad,
    rowVirtualizer,
    runIdsKey,
    selectedGroupId,
    selectedRunId,
    selectedTaskId,
    sharesScroller,
    taskNameColumnRef,
  ]);
};
