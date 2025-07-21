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
import React, { useCallback, useMemo, useState } from "react";
import { useLocation, useParams } from "react-router-dom";

import type { GridTask, RunWithDuration } from "src/layouts/Details/Grid/utils";

import { NavigationModeContextValue } from "./Context";
import type { NavigationMode, NavigationModeContext, NavigationState } from "./types";

type Props = {
  readonly children: React.ReactNode;
  readonly flatNodes?: Array<GridTask>;
  readonly runs?: Array<RunWithDuration>;
};

const getNavigationStateFromMode = (mode: NavigationMode): NavigationState => {
  switch (mode) {
    case "grid":
      return {
        canNavigateHorizontal: true,
        canNavigateVertical: true,
        mode,
      };
    case "run":
      return {
        canNavigateHorizontal: true,
        canNavigateVertical: false,
        mode,
      };
    case "task":
      return {
        canNavigateHorizontal: false,
        canNavigateVertical: true,
        mode,
      };
    default:
      return {
        canNavigateHorizontal: true,
        canNavigateVertical: true,
        mode: "grid",
      };
  }
};

const detectNavigationModeFromUrl = (pathname: string): NavigationMode => {
  // Pattern: /dags/{dagId}/runs/{runId}/tasks/{taskId} -> grid mode
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

const defaultFlatNodes: Array<GridTask> = [];
const defaultRuns: Array<RunWithDuration> = [];

export const NavigationModeProvider = ({ children, flatNodes = defaultFlatNodes, runs = defaultRuns }: Props) => {
  const location = useLocation();
  const { dagId = "", groupId = "", runId = "", taskId = "" } = useParams();

  const [navigationMode, setNavigationMode] = useState<NavigationMode>(() =>
    detectNavigationModeFromUrl(location.pathname)
  );

  const navigationState = useMemo(
    () => getNavigationStateFromMode(navigationMode),
    [navigationMode]
  );

  const setNavigationModeCallback = useCallback((mode: NavigationMode) => {
    setNavigationMode(mode);
  }, []);

  const getNavigationUrl = useCallback((direction: "down" | "left" | "right" | "up"): string | undefined => {
    const currentTaskId = groupId || taskId;

    switch (navigationState.mode) {
      case "grid": {
        // Grid mode: navigate taskInstance combinations
        const currentRunIndex = runs.findIndex(run => run.dag_run_id === runId);
        const currentTaskIndex = flatNodes.findIndex(node => node.id === currentTaskId);

        let newRunIndex = currentRunIndex;
        let newTaskIndex = currentTaskIndex;

        if (direction === "left" || direction === "right") {
          const step = direction === "left" ? 1 : -1;

          newRunIndex = Math.max(0, Math.min(runs.length - 1, currentRunIndex + step));
        }

        if (direction === "up" || direction === "down") {
          const step = direction === "up" ? -1 : 1;

          newTaskIndex = Math.max(0, Math.min(flatNodes.length - 1, currentTaskIndex + step));
        }

        const targetRun = runs[newRunIndex];
        const targetTask = flatNodes[newTaskIndex];

        if (targetRun && targetTask) {
          const groupPath = targetTask.isGroup ? "group/" : "";

          return `/dags/${dagId}/runs/${targetRun.dag_run_id}/tasks/${groupPath}${targetTask.id}`;
        }

        break;
      }

      case "run": {
        // Run mode: only left/right navigation
        if (direction === "left" || direction === "right") {
          const currentRunIndex = runs.findIndex(run => run.dag_run_id === runId);
          const step = direction === "left" ? 1 : -1;
          const newRunIndex = Math.max(0, Math.min(runs.length - 1, currentRunIndex + step));
          const targetRun = runs[newRunIndex];

          if (targetRun) {

            return `/dags/${dagId}/runs/${targetRun.dag_run_id}/`;
          }
        }

        break;
      }

      case "task": {
        // Task mode: only up/down navigation
        if (direction === "up" || direction === "down") {
          const currentTaskIndex = flatNodes.findIndex(node => node.id === currentTaskId);
          const step = direction === "up" ? -1 : 1;
          const newTaskIndex = Math.max(0, Math.min(flatNodes.length - 1, currentTaskIndex + step));
          const targetTask = flatNodes[newTaskIndex];

          if (targetTask) {
            const groupPath = targetTask.isGroup ? "group/" : "";

            return `/dags/${dagId}/tasks/${groupPath}${targetTask.id}`;
          }
        }

        break;
      }

      default:
        // Fallback case
        break;
    }

    return undefined;
  }, [navigationState.mode, dagId, groupId, runId, taskId, runs, flatNodes]);

  const contextValue = useMemo<NavigationModeContext>(
    () => ({
      getNavigationUrl,
      navigationState,
      setNavigationMode: setNavigationModeCallback,
    }),
    [navigationState, setNavigationModeCallback, getNavigationUrl]
  );

  return (
    <NavigationModeContextValue.Provider value={contextValue}>
      {children}
    </NavigationModeContextValue.Provider>
  );
};
