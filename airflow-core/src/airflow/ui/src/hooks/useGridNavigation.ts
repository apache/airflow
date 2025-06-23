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
import { useHotkeys } from "react-hotkeys-hook";
import { useNavigate, useParams } from "react-router-dom";

import type { GridTask, RunWithDuration } from "src/layouts/Details/Grid/utils";
import { getTaskNavigationPath } from "src/utils/links";

type UseGridNavigationProps = {
  flatNodes: Array<GridTask>;
  isGridFocused: boolean;
  runs: Array<RunWithDuration>;
};

export const useGridNavigation = ({ flatNodes, isGridFocused, runs }: UseGridNavigationProps) => {
  const navigate = useNavigate();
  const { dagId = "", runId = "", taskId = "" } = useParams();

  const getCurrentIndices = useCallback(() => {
    const currentRunIndex = runs.findIndex((run) => run.dag_run_id === runId);
    const currentTaskIndex = flatNodes.findIndex((node) => node.id === taskId);

    return {
      runIndex: Math.max(0, currentRunIndex),
      taskIndex: Math.max(0, currentTaskIndex),
    };
  }, [runs, flatNodes, runId, taskId]);

  const navigateToPosition = useCallback(
    (runIndex: number, taskIndex: number) => {
      if (runs.length === 0 || flatNodes.length === 0) {
        return;
      }

      const run = runs[runIndex];
      const task = flatNodes[taskIndex];

      if (!run || !task) {
        return;
      }

      const { search } = globalThis.location;
      const searchParams = new URLSearchParams(search);
      const path = getTaskNavigationPath({
        dagId,
        isGroup: task.isGroup,
        isMapped: Boolean(task.is_mapped),
        runId: run.dag_run_id,
        taskId: task.id,
      });

      navigate(
        {
          pathname: path,
          search: searchParams.toString(),
        },
        { replace: true },
      );
    },
    [navigate, dagId, runs, flatNodes],
  );

  const handleKeyNavigation = useCallback(
    (key: "ArrowDown" | "ArrowUp" | "ArrowLeft" | "ArrowRight", isQuickJump: boolean) => {
      const { runIndex, taskIndex } = getCurrentIndices();
      
      const navigationConfig = {
        ArrowDown: {
          runIndex,
          taskIndex: isQuickJump ? flatNodes.length - 1 : Math.min(flatNodes.length - 1, taskIndex + 1),
        },
        ArrowUp: {
          runIndex,
          taskIndex: isQuickJump ? 0 : Math.max(0, taskIndex - 1),
        },
        ArrowLeft: {
          runIndex: isQuickJump ? runs.length - 1 : Math.min(runs.length - 1, runIndex + 1),
          taskIndex,
        },
        ArrowRight: {
          runIndex: isQuickJump ? 0 : Math.max(0, runIndex - 1),
          taskIndex,
        },
      };

      const { runIndex: newRunIndex, taskIndex: newTaskIndex } = navigationConfig[key];
      navigateToPosition(newRunIndex, newTaskIndex);
    },
    [getCurrentIndices, navigateToPosition, flatNodes.length, runs.length],
  );


  useHotkeys(
    [
      "ArrowDown", "meta+ArrowDown", "ctrl+ArrowDown",
      "ArrowUp", "meta+ArrowUp", "ctrl+ArrowUp",
      "ArrowLeft", "meta+ArrowLeft", "ctrl+ArrowLeft", 
      "ArrowRight", "meta+ArrowRight", "ctrl+ArrowRight",
    ],
    (event, _handler) => {
      event.stopPropagation();
      const isQuickJump = event.metaKey || event.ctrlKey;
      handleKeyNavigation(event.key as "ArrowDown" | "ArrowUp" | "ArrowLeft" | "ArrowRight", isQuickJump);
    },
    {
      enabled: isGridFocused,
      preventDefault: true,
    },
  );

  return {
    getCurrentIndices,
    navigateToPosition,
  };
};
