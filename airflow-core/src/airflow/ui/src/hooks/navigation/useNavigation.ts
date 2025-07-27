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

import type {
  ClickTarget,
  NavigationDirection,
  NavigationIndices,
  NavigationMode,
  UseNavigationProps,
  UseNavigationReturn,
} from "./types";
import { useKeyboardNavigation } from "./useKeyboardNavigation";
import { useNavigationCalculation } from "./useNavigationCalculation";

const detectModeFromClick = (target: ClickTarget): NavigationMode => {
  switch (target) {
    case "column":
      return "run";
    case "grid":
      return "grid";
    case "row":
      return "task";
    default:
      return "grid";
  }
};

const detectModeFromUrl = (pathname: string): NavigationMode => {
  if (pathname.includes("/runs/") && pathname.includes("/tasks/")) {
    return "grid";
  }
  if (pathname.includes("/runs/") && !pathname.includes("/tasks/")) {
    return "run";
  }
  if (pathname.includes("/tasks/") && !pathname.includes("/runs/")) {
    return "task";
  }

  return "grid";
};

export const useNavigation = ({ enabled = true, runs, tasks }: UseNavigationProps): UseNavigationReturn => {
  const { dagId = "", groupId = "", runId = "", taskId = "" } = useParams();
  const navigate = useNavigate();
  const [isNavigating, setIsNavigating] = useState(false);
  const [mode, setMode] = useState<NavigationMode>("grid");

  // Auto-detect mode from URL
  useEffect(() => {
    if (typeof globalThis !== "undefined") {
      const detectedMode = detectModeFromUrl(globalThis.location.pathname);

      setMode(detectedMode);
    }
  }, [dagId, groupId, runId, taskId]);

  // Calculate current indices from URL params
  const currentIndices = useMemo((): NavigationIndices => {
    const runIndex = Math.max(
      0,
      runs.findIndex((run) => run.run_id === runId),
    );

    const currentTaskId = groupId || taskId;
    const taskIndex = Math.max(
      0,
      tasks.findIndex((task) => task.id === currentTaskId),
    );

    return { runIndex, taskIndex };
  }, [groupId, runId, runs, taskId, tasks]);

  const { getNavigationTarget } = useNavigationCalculation({ mode, runs, tasks });

  const setModeFromClick = useCallback((target: ClickTarget) => {
    const newMode = detectModeFromClick(target);

    setMode(newMode);
  }, []);

  const handleNavigation = useCallback(
    (direction: NavigationDirection, isJump: boolean = false) => {
      if (!enabled || !dagId) {
        return;
      }

      setIsNavigating(true);

      const target = getNavigationTarget(currentIndices, direction, isJump);

      if (target.isValid) {
        navigate(target.path, { replace: true });
      }

      setTimeout(() => {
        setIsNavigating(false);
      }, 100);
    },
    [currentIndices, dagId, enabled, getNavigationTarget, navigate],
  );

  useKeyboardNavigation({
    enabled: enabled && Boolean(dagId),
    mode,
    onNavigate: handleNavigation,
  });

  return {
    currentIndices,
    enabled: enabled && Boolean(dagId),
    handleNavigation,
    isNavigating,
    mode,
    setMode: setModeFromClick,
  };
};
