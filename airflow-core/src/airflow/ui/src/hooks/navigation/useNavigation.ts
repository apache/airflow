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
import { useNavigate, useParams } from "react-router-dom";

import type { GridRunsResponse } from "openapi/requests";
import type { GridTask } from "src/layouts/Details/Grid/utils";
import { getTaskInstanceLinkFromObj } from "src/utils/links";

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

const getNextIndex = (
  current: number,
  direction: number,
  options: { isJump: boolean; max: number },
): number => {
  if (options.isJump) {
    return direction > 0 ? options.max - 1 : 0;
  }

  return Math.max(0, Math.min(options.max - 1, current + direction));
};

const buildPath = (params: {
  dagId: string;
  mapIndex?: string;
  mode: NavigationMode;
  run: GridRunsResponse;
  task: GridTask;
}): string => {
  const { dagId, mapIndex = "-1", mode, run, task } = params;
  const groupPath = task.isGroup ? "group/" : "";

  switch (mode) {
    case "run":
      return `/dags/${dagId}/runs/${run.run_id}`;
    case "task":
      return `/dags/${dagId}/tasks/${groupPath}${task.id}`;
    case "TI":
      if (task.is_mapped ?? false) {
        if (mapIndex !== "-1") {
          return getTaskInstanceLinkFromObj({
            dagId,
            dagRunId: run.run_id,
            mapIndex: parseInt(mapIndex, 10),
            taskId: `${groupPath}${task.id}`,
          });
        }

        return `/dags/${dagId}/runs/${run.run_id}/tasks/${groupPath}${task.id}/mapped`;
      }

      return `/dags/${dagId}/runs/${run.run_id}/tasks/${groupPath}${task.id}`;
    default:
      return `/dags/${dagId}`;
  }
};

export const useNavigation = ({
  enabled = true,
  onEscapePress,
  onToggleGroup,
  runs,
  tasks,
}: UseNavigationProps): UseNavigationReturn => {
  const { dagId = "", groupId = "", mapIndex = "-1", runId = "", taskId = "" } = useParams();
  const navigate = useNavigate();
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

  const handleNavigation = useCallback(
    (direction: NavigationDirection, isJump: boolean = false) => {
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

      if (!isJump && isAtBoundary) {
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
          isJump,
          max: nav.max,
        });
      } else {
        newIndices.runIndex = getNextIndex(currentIndices.runIndex, nav.direction, { isJump, max: nav.max });
      }

      const { runIndex: newRunIndex, taskIndex: newTaskIndex } = newIndices;

      if (newRunIndex === currentIndices.runIndex && newTaskIndex === currentIndices.taskIndex) {
        return;
      }

      const run = runs[newRunIndex];
      const task = tasks[newTaskIndex];

      if (run && task) {
        const path = buildPath({ dagId, mapIndex, mode, run, task });

        navigate(path, { replace: true });
      }
    },
    [currentIndices, dagId, enabled, mapIndex, mode, runs, tasks, navigate],
  );

  useKeyboardNavigation({
    enabled: enabled && Boolean(dagId),
    onEscapePress,
    onNavigate: handleNavigation,
    onToggleGroup: currentTask?.isGroup && onToggleGroup ? () => onToggleGroup(currentTask.id) : undefined,
  });

  return {
    currentIndices,
    currentTask,
    enabled: enabled && Boolean(dagId),
    handleNavigation,
    mode,
    setMode,
  };
};
