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
import { useCallback, useMemo } from "react";
import { useHotkeys } from "react-hotkeys-hook";
import { useLocation, useNavigate, useParams, useSearchParams } from "react-router-dom";

import type { GridTask, RunWithDuration } from "src/layouts/Details/Grid/utils";

import type { ArrowKey } from "./NavigationCalculator";

const ARROW_KEYS = ["ArrowDown", "ArrowUp", "ArrowLeft", "ArrowRight"] as const;

type NavigationMode = "grid" | "run" | "task";

type Props = {
  flatNodes: Array<GridTask>;
  isGridFocused: boolean;
  runs: Array<RunWithDuration>;
};

const detectNavigationMode = (pathname: string): NavigationMode => {
  // Pattern: /dags/{dagId}/runs/{runId}/tasks/{taskId} -> grid mode (taskInstance)
  if (pathname.includes("/runs/") && pathname.includes("/tasks/")) {
    return "grid";
  }
  // Pattern: /dags/{dagId}/runs/{runId} -> run mode
  if (pathname.includes("/runs/") && !pathname.includes("/tasks/")) {
    return "run";
  }
  // Pattern: /dags/{dagId}/tasks/{taskId} -> task mode
  if (pathname.includes("/tasks/") && !pathname.includes("/runs/")) {
    return "task";
  }

  return "grid"; // Default fallback
};

export const useSmartNavigation = ({ flatNodes, isGridFocused, runs }: Props) => {
  const location = useLocation();
  const navigate = useNavigate();
  const [searchParams] = useSearchParams();
  const { dagId = "", groupId = "", runId = "", taskId = "" } = useParams();

  const navigationMode = useMemo(() => detectNavigationMode(location.pathname), [location.pathname]);

  const navigateInRunMode = useCallback((direction: "left" | "right") => {
    const currentRunIndex = runs.findIndex(run => run.dag_run_id === runId);
    const step = direction === "left" ? 1 : -1;
    const newRunIndex = Math.max(0, Math.min(runs.length - 1, currentRunIndex + step));
    const targetRun = runs[newRunIndex];

    if (targetRun && targetRun.dag_run_id !== runId) {
      navigate({
        pathname: `/dags/${dagId}/runs/${targetRun.dag_run_id}/`,
        search: searchParams.toString()
      }, { replace: true });
    }
  }, [runs, runId, navigate, dagId, searchParams]);

  const navigateInTaskMode = useCallback((direction: "down" | "up") => {
    const currentTaskId = groupId || taskId;
    const currentTaskIndex = flatNodes.findIndex(node => node.id === currentTaskId);
    const step = direction === "up" ? -1 : 1;
    const newTaskIndex = Math.max(0, Math.min(flatNodes.length - 1, currentTaskIndex + step));
    const targetTask = flatNodes[newTaskIndex];

    if (targetTask && targetTask.id !== currentTaskId) {
      const groupPath = targetTask.isGroup ? "group/" : "";

      navigate({
        pathname: `/dags/${dagId}/tasks/${groupPath}${targetTask.id}`,
        search: searchParams.toString()
      }, { replace: true });
    }
  }, [flatNodes, groupId, taskId, navigate, dagId, searchParams]);

  const handleKeyNavigation = useCallback((key: ArrowKey) => {
    // Only handle navigation for run and task modes
    // Grid mode is handled by the original useGridNavigation
    if (navigationMode === "run") {
      if (key === "ArrowLeft" || key === "ArrowRight") {
        navigateInRunMode(key === "ArrowLeft" ? "left" : "right");
      }
    } else if (navigationMode === "task") {
      if (key === "ArrowUp" || key === "ArrowDown") {
        navigateInTaskMode(key === "ArrowUp" ? "up" : "down");
      }
    }
  }, [navigationMode, navigateInRunMode, navigateInTaskMode]);

  useHotkeys(
    ARROW_KEYS,
    (event) => {
      // Only intercept for run and task modes
      if (navigationMode === "run" || navigationMode === "task") {
        event.preventDefault();
        event.stopPropagation();
        handleKeyNavigation(event.key as ArrowKey);
      }
    },
    {
      enabled: isGridFocused && (navigationMode === "run" || navigationMode === "task"),
      preventDefault: true,
    }
  );

  return {
    navigationMode,
  };
};
