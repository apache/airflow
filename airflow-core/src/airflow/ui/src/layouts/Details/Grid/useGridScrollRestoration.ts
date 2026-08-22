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
import { useEffect, useLayoutEffect, useRef } from "react";
import { useLocation } from "react-router-dom";

/**
 * Grid vertical scroll offset per ``dagId``, kept at module scope so it survives the
 * remount that happens when navigating between the sibling ``<Dag>`` / ``<Run>`` /
 * ``<Task>`` / ``<TaskInstance>`` routes — each mounts its own ``Grid``, which would
 * otherwise start at ``scrollTop = 0``.
 */
const gridScrollOffsetByDagId = new Map<string, number>();

// Matches a specific Dag (`/dags/<id>...`) but not the `/dags` list.
const DAG_DETAILS_PATH = /^\/dags\/(?<dagId>[^/]+)/u;

export const extractDagIdFromPath = (pathname: string): string | undefined =>
  DAG_DETAILS_PATH.exec(pathname)?.groups?.dagId;

export const saveGridScrollOffset = (dagId: string, offset: number): void => {
  if (dagId !== "") {
    gridScrollOffsetByDagId.set(dagId, offset);
  }
};

export const readGridScrollOffset = (dagId: string): number | undefined => gridScrollOffsetByDagId.get(dagId);

export const clearGridScrollOffsets = (): void => gridScrollOffsetByDagId.clear();

type Params = {
  readonly dagId: string;
  readonly getScrollElement: () => HTMLElement | null;
  /** Number of rendered rows — restore waits until the list has rows (and thus height). */
  readonly rowCount: number;
};

/**
 * Preserves the grid's vertical scroll position across the route transitions that
 * remount the grid. Saves ``scrollTop`` on every scroll and restores it once, after
 * the rows exist, so the remount is invisible to the user.
 */
export const useGridScrollRestoration = ({ dagId, getScrollElement, rowCount }: Params): void => {
  const hasRestoredRef = useRef(false);

  useEffect(() => {
    const element = getScrollElement();

    if (element === null || dagId === "") {
      return undefined;
    }

    const handleScroll = () => saveGridScrollOffset(dagId, element.scrollTop);

    element.addEventListener("scroll", handleScroll, { passive: true });

    return () => element.removeEventListener("scroll", handleScroll);
  }, [dagId, getScrollElement]);

  // Layout effect (not a plain effect) so the position is set before paint, avoiding
  // a visible flash at the top of the list on remount.
  useLayoutEffect(() => {
    if (hasRestoredRef.current || dagId === "" || rowCount === 0) {
      return;
    }

    const element = getScrollElement();
    const saved = readGridScrollOffset(dagId);

    if (element !== null && saved !== undefined && saved > 0) {
      element.scrollTop = saved;
    }

    hasRestoredRef.current = true;
  }, [dagId, getScrollElement, rowCount]);
};

// Call once from the app shell, so an offset never outlives the Dag visit that produced it.
export const useResetGridScrollOnLeave = (): void => {
  const { pathname } = useLocation();
  const dagId = extractDagIdFromPath(pathname);

  // Keyed on the Dag, not the pathname: moving within one Dag (grid → task → run) must keep the
  // offset, which is the whole point of the restore.
  useEffect(() => {
    clearGridScrollOffsets();
  }, [dagId]);
};
