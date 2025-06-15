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
import { useCallback, useEffect } from "react";
import { useNavigate, useParams } from "react-router-dom";

import type { GridTask, RunWithDuration } from "src/layouts/Details/Grid/utils";

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
      const groupPath = task.isGroup ? "group/" : "";
      const mappedPath = task.is_mapped ? "/mapped" : "";
      const path = `/dags/${dagId}/runs/${run.dag_run_id}/tasks/${groupPath}${task.id}${mappedPath}`;

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

  const handleKeyDown = useCallback(
    (event: KeyboardEvent) => {
      const isArrowKey = ["ArrowDown", "ArrowLeft", "ArrowRight", "ArrowUp", "Enter"].includes(event.key);

      if (!isArrowKey || !isGridFocused) {
        return;
      }

      event.preventDefault();
      event.stopPropagation();

      const { runIndex, taskIndex } = getCurrentIndices();
      let newRunIndex = runIndex;
      let newTaskIndex = taskIndex;

      switch (event.key) {
        case "ArrowDown": {
          newTaskIndex = Math.min(flatNodes.length - 1, taskIndex + 1);
          break;
        }
        case "ArrowLeft": {
          newRunIndex = Math.min(runs.length - 1, runIndex + 1);
          break;
        }
        case "ArrowRight": {
          newRunIndex = Math.max(0, runIndex - 1);
          break;
        }
        case "ArrowUp": {
          newTaskIndex = Math.max(0, taskIndex - 1);
          break;
        }
        case "Enter": {
          navigateToPosition(runIndex, taskIndex);

          return;
        }
        default: {
          return;
        }
      }

      navigateToPosition(newRunIndex, newTaskIndex);
    },
    [isGridFocused, getCurrentIndices, navigateToPosition, flatNodes.length, runs.length],
  );

  useEffect(() => {
    document.addEventListener("keydown", handleKeyDown);

    return () => {
      document.removeEventListener("keydown", handleKeyDown);
    };
  }, [handleKeyDown]);

  return {
    getCurrentIndices,
    navigateToPosition,
  };
};
