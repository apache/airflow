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
import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { useLocation, useNavigate, useParams } from "react-router-dom";

import type { GridRunsResponse } from "openapi/requests";
import type { GridTask } from "src/layouts/Details/Grid/utils";
import { setRefOpacity, setRefTransformAndOpacity } from "src/utils/domUtils";
import { buildTaskInstanceUrl } from "src/utils/links";

import type {
  NavigationDirection,
  NavigationIndices,
  NavigationMode,
  UseNavigationProps,
  UseNavigationReturn,
} from "./types";
import { useKeyboardNavigation } from "./useKeyboardNavigation";

// Grid cell dimensions (must match Grid.tsx)
const CELL_WIDTH = 18;
const CELL_HEIGHT = 20;

const detectModeFromUrl = (pathname: string): NavigationMode => {
  if (pathname.includes("/runs/") && pathname.includes("/tasks/")) {
    return "TI";
  }
  if (pathname.includes("/runs/") && !pathname.includes("/tasks/")) {
    return "run";
  }
  if (pathname.includes("/tasks/") && !pathname.includes("/runs/")) {
    return "task";
  }

  return "TI";
};

const isValidDirection = (direction: NavigationDirection, mode: NavigationMode): boolean => {
  switch (mode) {
    case "run":
      return direction === "left" || direction === "right";
    case "task":
      return direction === "down" || direction === "up";
    case "TI":
      return true;
    default:
      return false;
  }
};

const getNextIndex = (current: number, direction: number, options: { max: number }): number =>
  Math.max(0, Math.min(options.max - 1, current + direction));

const buildPath = (params: {
  dagId: string;
  mapIndex?: string;
  mode: NavigationMode;
  pathname: string;
  run: GridRunsResponse;
  task: GridTask;
}): string => {
  const { dagId, mapIndex = "-1", mode, pathname, run, task } = params;
  const groupPath = task.isGroup ? "group/" : "";

  switch (mode) {
    case "run":
      return `/dags/${dagId}/runs/${run.run_id}`;
    case "task":
      return `/dags/${dagId}/tasks/${groupPath}${task.id}`;
    case "TI":
      return buildTaskInstanceUrl({
        currentPathname: pathname,
        dagId,
        isGroup: task.isGroup,
        isMapped: task.is_mapped ?? false,
        mapIndex,
        runId: run.run_id,
        taskId: task.id,
      });
    default:
      return `/dags/${dagId}`;
  }
};

export const useNavigation = ({
  navCellRef,
  navColRef,
  navRowRef,
  onToggleGroup,
  runs,
  tasks,
}: UseNavigationProps): UseNavigationReturn => {
  const { dagId = "", groupId = "", mapIndex = "-1", runId = "", taskId = "" } = useParams();
  const enabled = Boolean(dagId) && (Boolean(runId) || Boolean(taskId) || Boolean(groupId));
  const navigate = useNavigate();
  const location = useLocation();
  const [mode, setMode] = useState<NavigationMode>("TI");

  useEffect(() => {
    const detectedMode = detectModeFromUrl(globalThis.location.pathname);

    setMode(detectedMode);
  }, [dagId, groupId, runId, taskId]);

  const currentIndices = useMemo((): NavigationIndices => {
    const runMap = new Map(runs.map((run, index) => [run.run_id, index]));
    const taskMap = new Map(tasks.map((task, index) => [task.id, index]));

    const runIndex = runMap.get(runId) ?? 0;
    const currentTaskId = groupId || taskId;
    const taskIndex = taskMap.get(currentTaskId) ?? 0;

    return { runIndex, taskIndex };
  }, [groupId, runId, runs, taskId, tasks]);

  const currentTask = useMemo(() => tasks[currentIndices.taskIndex], [tasks, currentIndices.taskIndex]);

  // Ref to track preview position during key hold (no re-render)
  const previewIndicesRef = useRef<NavigationIndices>(currentIndices);

  // Sync ref with actual indices when URL changes
  useEffect(() => {
    previewIndicesRef.current = currentIndices;
  }, [currentIndices]);

  // Clear navigation highlight overlays
  const clearHighlight = useCallback(() => {
    setRefOpacity(navRowRef, "0");
    setRefOpacity(navColRef, "0");
    setRefOpacity(navCellRef, "0");
  }, [navCellRef, navColRef, navRowRef]);

  // Apply highlight using direct DOM manipulation (GPU composited)
  const applyHighlight = useCallback(
    (indices: NavigationIndices, navMode: NavigationMode) => {
      const { runIndex, taskIndex } = indices;

      // Calculate positions (same logic as hover)
      const rowY = taskIndex * CELL_HEIGHT;
      const colX = -(runIndex * CELL_WIDTH);

      const showRow = navMode === "task" || navMode === "TI";
      const showCol = navMode === "run" || navMode === "TI";

      // Update row highlight
      setRefTransformAndOpacity(
        navRowRef,
        showRow ? `translateY(${rowY}px)` : undefined,
        showRow ? "1" : "0",
      );

      // Update column highlight
      setRefTransformAndOpacity(
        navColRef,
        showCol ? `translateX(${colX}px)` : undefined,
        showCol ? "1" : "0",
      );

      // Update cell highlight (TI mode only)
      setRefTransformAndOpacity(
        navCellRef,
        navMode === "TI" ? `translate(${colX}px, ${rowY}px)` : undefined,
        navMode === "TI" ? "1" : "0",
      );
    },
    [navCellRef, navColRef, navRowRef],
  );

  // Preview navigation: update ref + overlay highlight (keydown)
  const handleNavigation = useCallback(
    (direction: NavigationDirection) => {
      if (!enabled || !dagId || !isValidDirection(direction, mode)) {
        return;
      }

      const prevIndices = previewIndicesRef.current;

      const boundaries = {
        down: prevIndices.taskIndex >= tasks.length - 1,
        left: prevIndices.runIndex >= runs.length - 1,
        right: prevIndices.runIndex <= 0,
        up: prevIndices.taskIndex <= 0,
      };

      if (boundaries[direction]) {
        return;
      }

      const navigationMap: Record<
        NavigationDirection,
        { direction: number; index: "runIndex" | "taskIndex"; max: number }
      > = {
        down: { direction: 1, index: "taskIndex", max: tasks.length },
        left: { direction: 1, index: "runIndex", max: runs.length },
        right: { direction: -1, index: "runIndex", max: runs.length },
        up: { direction: -1, index: "taskIndex", max: tasks.length },
      };

      const nav = navigationMap[direction];
      const newIndices = { ...prevIndices };

      if (nav.index === "taskIndex") {
        newIndices.taskIndex = getNextIndex(prevIndices.taskIndex, nav.direction, { max: nav.max });
      } else {
        newIndices.runIndex = getNextIndex(prevIndices.runIndex, nav.direction, { max: nav.max });
      }

      if (newIndices.runIndex === prevIndices.runIndex && newIndices.taskIndex === prevIndices.taskIndex) {
        return;
      }

      // Update ref (no re-render)
      previewIndicesRef.current = newIndices;

      // Apply highlight instantly via direct DOM manipulation
      applyHighlight(newIndices, mode);
    },
    [applyHighlight, dagId, enabled, mode, runs, tasks],
  );

  // Commit navigation: URL update (keyup)
  const commitNavigation = useCallback(() => {
    const indices = previewIndicesRef.current;
    const run = runs[indices.runIndex];
    const task = tasks[indices.taskIndex];

    // Clear highlight
    clearHighlight();

    if (run && task) {
      const path = buildPath({ dagId, mapIndex, mode, pathname: location.pathname, run, task });

      navigate(path, { replace: true });

      const grid = document.querySelector(`[id='grid-${run.run_id}-${task.id}']`);

      if (grid) {
        (grid as HTMLLinkElement).focus();
      }
    }
  }, [clearHighlight, dagId, location.pathname, mapIndex, mode, navigate, runs, tasks]);

  useKeyboardNavigation({
    enabled,
    onCommit: commitNavigation,
    onNavigate: handleNavigation,
    onToggleGroup: currentTask?.isGroup && onToggleGroup ? () => onToggleGroup(currentTask.id) : undefined,
  });

  return {
    currentIndices,
    currentTask,
    handleNavigation,
    mode,
    setMode,
  };
};
