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
import { useEffect, useState } from "react";
import { useLocation, useNavigate, useParams } from "react-router-dom";

import { useDagRunUrlBuilder, useTaskInstanceUrlBuilder, useTaskUrlBuilder } from "src/hooks/useUrlBuilders";

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

export const useNavigation = ({ onToggleGroup, runs, tasks }: UseNavigationProps): UseNavigationReturn => {
  const { dagId = "", groupId = "", mapIndex = "-1", runId = "", taskId = "" } = useParams();
  const enabled = Boolean(dagId) && (Boolean(runId) || Boolean(taskId) || Boolean(groupId));
  const navigate = useNavigate();
  const location = useLocation();
  const [mode, setMode] = useState<NavigationMode>(NavigationModes.TI);

  // Use custom hooks for URL building
  const buildDagRunUrl = useDagRunUrlBuilder();
  const buildTaskUrl = useTaskUrlBuilder();
  const buildTaskInstanceUrl = useTaskInstanceUrlBuilder();

  useEffect(() => {
    const detectedMode = detectModeFromUrl(globalThis.location.pathname);

    setMode(detectedMode);
  }, [dagId, groupId, runId, taskId]);

  const runMap = new Map(runs.map((run, index) => [run.run_id, index]));
  const taskMap = new Map(tasks.map((task, index) => [task.id, index]));
  const runIndex = runMap.get(runId) ?? 0;
  const currentTaskId = groupId || taskId;
  const taskIndex = taskMap.get(currentTaskId) ?? 0;
  const currentIndices: NavigationIndices = { runIndex, taskIndex };

  const currentTask = tasks[currentIndices.taskIndex];

  const handleNavigation = (direction: NavigationDirection) => {
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
        let path: string;

        switch (mode) {
          case "run":
            path = buildDagRunUrl(run.run_id);
            break;
          case "task":
            path = buildTaskUrl({
              isGroup: task.isGroup,
              taskId: task.id,
            });
            break;
          case "TI":
            path = buildTaskInstanceUrl({
              isGroup: task.isGroup,
              isMapped: task.is_mapped ?? false,
              mapIndex,
              runId: run.run_id,
              taskId: task.id,
            });
            break;
          default:
            path = `/dags/${dagId}`;
        }

      void Promise.resolve(navigate(path, { replace: true }));

      const grid = document.querySelector(`[id='grid-${run.run_id}-${task.id}']`);

      // Set the focus to the grid link to allow a user to continue tabbing through with the keyboard
      if (grid) {
        (grid as HTMLLinkElement).focus();
      }
    },
    [
      buildDagRunUrl,
      buildTaskInstanceUrl,
      buildTaskUrl,
      currentIndices,
      dagId,
      enabled,
      mapIndex,
      mode,
      navigate,
      runs,
      tasks,
    ],
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
