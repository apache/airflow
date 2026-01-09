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
import { useCallback, useEffect, useMemo, useState } from "react";
import { useLocation, useNavigate, useParams } from "react-router-dom";

import type { GridRunsResponse } from "openapi/requests";
import type { GridTask } from "src/layouts/Details/Grid/utils";
import { buildTaskInstanceUrl } from "src/utils/links";

import {
  NavigationModes,
  type NavigationDirection,
  type NavigationIndices,
  type NavigationMode,
  type UseNavigationProps,
  type UseNavigationReturn,
} from "./types";
import { useKeyboardNavigation } from "./useKeyboardNavigation";

const detectModeFromUrl = (pathname: string): NavigationMode => {
  if (pathname.includes("/runs/") && pathname.includes("/tasks/")) {
    return NavigationModes.TI;
  }
  if (pathname.includes("/runs/") && !pathname.includes("/tasks/")) {
    return NavigationModes.RUN;
  }
  if (pathname.includes("/tasks/") && !pathname.includes("/runs/")) {
    return NavigationModes.TASK;
  }

  return NavigationModes.TI;
};

const isValidDirection = (direction: NavigationDirection, mode: NavigationMode): boolean => {
  switch (mode) {
    case NavigationModes.RUN:
      return direction === "left" || direction === "right";
    case NavigationModes.TASK:
      return direction === "down" || direction === "up";
    case NavigationModes.TI:
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
    case NavigationModes.RUN:
      return `/dags/${dagId}/runs/${run.run_id}`;
    case NavigationModes.TASK:
      return `/dags/${dagId}/tasks/${groupPath}${task.id}`;
    case NavigationModes.TI:
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

export const useNavigation = ({ onToggleGroup, runs, tasks }: UseNavigationProps): UseNavigationReturn => {
  const { dagId = "", groupId = "", mapIndex = "-1", runId = "", taskId = "" } = useParams();
  const enabled = Boolean(dagId) && (Boolean(runId) || Boolean(taskId) || Boolean(groupId));
  const navigate = useNavigate();
  const location = useLocation();
  const [mode, setMode] = useState<NavigationMode>(NavigationModes.TI);

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

  const handleNavigation = useCallback(
    (direction: NavigationDirection) => {
      if (!enabled || !dagId || !isValidDirection(direction, mode)) {
        return;
      }

      const boundaries = {
        down: currentIndices.taskIndex >= tasks.length - 1,
        left: currentIndices.runIndex >= runs.length - 1,
        right: currentIndices.runIndex <= 0,
        up: currentIndices.taskIndex <= 0,
      };

      const isAtBoundary = boundaries[direction];

      if (isAtBoundary) {
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

      const newIndices = { ...currentIndices };

      if (nav.index === "taskIndex") {
        newIndices.taskIndex = getNextIndex(currentIndices.taskIndex, nav.direction, {
          max: nav.max,
        });
      } else {
        newIndices.runIndex = getNextIndex(currentIndices.runIndex, nav.direction, { max: nav.max });
      }

      const { runIndex: newRunIndex, taskIndex: newTaskIndex } = newIndices;

      if (newRunIndex === currentIndices.runIndex && newTaskIndex === currentIndices.taskIndex) {
        return;
      }

      const run = runs[newRunIndex];
      const task = tasks[newTaskIndex];

      if (run && task) {
        const path = buildPath({ dagId, mapIndex, mode, pathname: location.pathname, run, task });

        navigate(path, { replace: true });

        const grid = document.querySelector(`[id='grid-${run.run_id}-${task.id}']`);

        // Set the focus to the grid link to allow a user to continue tabbing through with the keyboard
        if (grid) {
          (grid as HTMLLinkElement).focus();
        }
      }
    },
    [currentIndices, dagId, enabled, location.pathname, mapIndex, mode, runs, tasks, navigate],
  );

  useKeyboardNavigation({
    enabled,
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
