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
import { useCallback } from "react";
import { useParams } from "react-router-dom";

import type { GridRunsResponse } from "openapi/requests";
import type { GridTask } from "src/layouts/Details/Grid/utils";

import type { NavigationDirection, NavigationIndices, NavigationMode, NavigationTarget } from "./types";

type Props = {
  mode: NavigationMode;
  runs: Array<GridRunsResponse>;
  tasks: Array<GridTask>;
};

type GetNextIndexOptions = {
  current: number;
  direction: -1 | 1;
  isJump: boolean;
  maxLength: number;
};

const getNextIndex = ({ current, direction, isJump, maxLength }: GetNextIndexOptions): number => {
  if (isJump) {
    return direction > 0 ? maxLength - 1 : 0;
  }

  return Math.max(0, Math.min(maxLength - 1, current + direction));
};

const isValidDirectionForMode = (direction: NavigationDirection, mode: NavigationMode): boolean => {
  switch (mode) {
    case "grid":
      return true;
    case "run":
      return direction === "left" || direction === "right";
    case "task":
      return direction === "down" || direction === "up";
    default:
      return false;
  }
};

export const useNavigationCalculation = ({ mode, runs, tasks }: Props) => {
  const { dagId = "" } = useParams();

  const calculateNewIndices = useCallback(
    (
      currentIndices: NavigationIndices,
      direction: NavigationDirection,
      isJump: boolean = false,
    ): NavigationIndices => {
      if (!isValidDirectionForMode(direction, mode)) {
        return currentIndices;
      }

      switch (direction) {
        case "down":
          return {
            ...currentIndices,
            taskIndex: getNextIndex({
              current: currentIndices.taskIndex,
              direction: 1,
              isJump,
              maxLength: tasks.length,
            }),
          };
        case "left":
          return {
            ...currentIndices,
            runIndex: getNextIndex({
              current: currentIndices.runIndex,
              direction: 1,
              isJump,
              maxLength: runs.length,
            }),
          };
        case "right":
          return {
            ...currentIndices,
            runIndex: getNextIndex({
              current: currentIndices.runIndex,
              direction: -1,
              isJump,
              maxLength: runs.length,
            }),
          };
        case "up":
          return {
            ...currentIndices,
            taskIndex: getNextIndex({
              current: currentIndices.taskIndex,
              direction: -1,
              isJump,
              maxLength: tasks.length,
            }),
          };
        default:
          return currentIndices;
      }
    },
    [mode, runs.length, tasks.length],
  );

  const buildNavigationPath = useCallback(
    (run: GridRunsResponse, task: GridTask): string => {
      switch (mode) {
        case "grid":
          const taskGroupPath = task.isGroup ? "group/" : "";

          return `/dags/${dagId}/runs/${run.run_id}/tasks/${taskGroupPath}${task.id}`;
        case "run":
          return `/dags/${dagId}/runs/${run.run_id}`;
        case "task":
          const groupPath = task.isGroup ? "group/" : "";

          return `/dags/${dagId}/tasks/${groupPath}${task.id}`;
        default:
          return `/dags/${dagId}`;
      }
    },
    [dagId, mode],
  );

  const getNavigationTarget = useCallback(
    (
      currentIndices: NavigationIndices,
      direction: NavigationDirection,
      isJump: boolean = false,
    ): NavigationTarget => {
      const newIndices = calculateNewIndices(currentIndices, direction, isJump);
      const run = runs[newIndices.runIndex];
      const task = tasks[newIndices.taskIndex];

      if (!run || !task) {
        return {
          indices: currentIndices,
          isValid: false,
          path: "",
          run: undefined,
          task: undefined,
        };
      }

      const path = buildNavigationPath(run, task);

      return {
        indices: newIndices,
        isValid: true,
        path,
        run,
        task,
      };
    },
    [buildNavigationPath, calculateNewIndices, runs, tasks],
  );

  return {
    calculateNewIndices,
    getNavigationTarget,
  };
};
