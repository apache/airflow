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
import { useLayoutEffect } from "react";

import type { Virtualizer } from "@tanstack/react-virtual";

import { ROW_HEIGHT } from "./constants";
import type { GridTask } from "./utils";

// Last Grid scrollTop per Dag. Module scope so it outlives the Grid remounting.
const gridScrollTops = new Map<string, number>();

type Params = {
  dagId: string;
  flatNodes: Array<GridTask>;
  // Height of the sticky header above the rows (also the virtualizer's scrollPaddingStart).
  headerPad: number;
  rowVirtualizer: Virtualizer<HTMLDivElement, Element>;
  selectedGroupId?: string;
  selectedTaskId?: string;
};

// Navigating into a task swaps the route (Dag/Run/TaskInstance each render their own Grid), so the
// Grid unmounts and a fresh one mounts scrolled to the top. We restore the last position here so a
// click doesn't jump the Grid: the rows are unchanged across the remount, so the saved pixel offset
// still points at the same rows.
//
// If the rows did change since we saved (a new version, or groups expanded/collapsed) the saved
// offset no longer lines up, so we fall back to locating the selected task by id and centering it.
// Rows are a fixed height, so the index alone gives the offset — no dependency on the stored pixels.
export const useGridScrollRestore = ({
  dagId,
  flatNodes,
  headerPad,
  rowVirtualizer,
  selectedGroupId,
  selectedTaskId,
}: Params) => {
  useLayoutEffect(() => {
    const scrollEl = rowVirtualizer.scrollElement;

    if (!scrollEl) {
      return undefined;
    }

    const saved = gridScrollTops.get(dagId);

    if (saved !== undefined) {
      scrollEl.scrollTop = saved;
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

    const handleScroll = () => gridScrollTops.set(dagId, scrollEl.scrollTop);

    scrollEl.addEventListener("scroll", handleScroll, { passive: true });

    return () => scrollEl.removeEventListener("scroll", handleScroll);
  }, [dagId, selectedTaskId, selectedGroupId, flatNodes, headerPad, rowVirtualizer]);
};
