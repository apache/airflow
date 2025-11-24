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
import { buildTaskInstanceUrl } from "src/utils/links";

import type {
  NavigationDirection,
  NavigationIndices,
  NavigationMode,
  UseNavigationProps,
  UseNavigationReturn,
} from "./types";
import { useKeyboardNavigation } from "./useKeyboardNavigation";

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

const HIGHLIGHT_CLASS = "nav-preview-highlight";

// Type for tracking highlighted elements (O(1) clear)
type HighlightedElements = {
  runColumn: HTMLElement | undefined;
  taskRow: HTMLElement | undefined;
};

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
  containerRef,
  interactionLayerRef,
  onToggleGroup,
  runs,
  tasks,
}: UseNavigationProps): UseNavigationReturn => {
  // Use overlay-based highlighting when interactionLayerRef is provided
  const useOverlay = Boolean(interactionLayerRef);
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

  // Track last highlighted elements for O(1) clear
  const highlightedRef = useRef<HighlightedElements>({ runColumn: undefined, taskRow: undefined });

  // Sync ref with actual indices when URL changes
  useEffect(() => {
    previewIndicesRef.current = currentIndices;
  }, [currentIndices]);

  // O(1) clear - only operates on tracked elements
  const clearHighlight = useCallback(() => {
    // Use overlay-based clearing if available
    if (useOverlay && interactionLayerRef) {
      interactionLayerRef.current?.clearHighlight();

      return;
    }

    // Fallback: DOM-based clearing
    const { runColumn, taskRow } = highlightedRef.current;

    if (taskRow) {
      taskRow.classList.remove(HIGHLIGHT_CLASS);
    }
    if (runColumn) {
      runColumn.classList.remove(HIGHLIGHT_CLASS);
    }
    highlightedRef.current = { runColumn: undefined, taskRow: undefined };
  }, [interactionLayerRef, useOverlay]);

  // Apply highlight - uses overlay when available, falls back to DOM queries
  const applyHighlight = useCallback(
    ({
      indices,
      navMode,
      targetRunId,
      targetTaskId,
    }: {
      indices: NavigationIndices;
      navMode: NavigationMode;
      targetRunId: string;
      targetTaskId: string;
    }) => {
      clearHighlight();

      // Use overlay-based highlighting if available (GPU compositing, O(1))
      if (useOverlay && interactionLayerRef) {
        const run = runs[indices.runIndex];
        const task = tasks[indices.taskIndex];

        if (run && task) {
          interactionLayerRef.current?.setHighlight(
            {
              colIndex: indices.runIndex,
              rowIndex: indices.taskIndex,
              runId: run.run_id,
              taskId: task.id,
            },
            navMode,
          );
        }

        return;
      }

      // Fallback: Scoped DOM queries - only searches within container
      const container: ParentNode = containerRef?.current ?? document;
      const highlightTask = navMode === "task" || navMode === "TI";
      const highlightRun = navMode === "run" || navMode === "TI";

      if (highlightTask) {
        const taskRow = container.querySelector<HTMLElement>(`[data-task-id='${targetTaskId}']`);

        if (taskRow !== null) {
          taskRow.classList.add(HIGHLIGHT_CLASS);
          taskRow.style.backgroundColor = "";
          highlightedRef.current.taskRow = taskRow;
        }
      }

      if (highlightRun) {
        const runColumn = container.querySelector<HTMLElement>(`[data-run-id='${targetRunId}']`);

        if (runColumn !== null) {
          runColumn.classList.add(HIGHLIGHT_CLASS);
          highlightedRef.current.runColumn = runColumn;
        }
      }
    },
    [clearHighlight, containerRef, interactionLayerRef, runs, tasks, useOverlay],
  );

  // Preview navigation: update ref + DOM highlight (keydown)
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

      // Apply highlight instantly (overlay or DOM-based)
      const run = runs[newIndices.runIndex];
      const task = tasks[newIndices.taskIndex];

      if (run && task) {
        applyHighlight({
          indices: newIndices,
          navMode: mode,
          targetRunId: run.run_id,
          targetTaskId: task.id,
        });
      }
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
