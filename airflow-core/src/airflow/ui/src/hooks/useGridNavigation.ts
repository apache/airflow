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
import { useNavigate, useParams } from "react-router-dom";

import type { GridTask, RunWithDuration } from "src/layouts/Details/Grid/utils";
import { getTaskNavigationPath } from "src/utils/links";

import { NavigationCalculator } from "./navigation/NavigationCalculator";
import { useNavigationKeyboard } from "./navigation/useNavigationKeyboard";
import { useNavigationPreviewEffect } from "./navigation/useNavigationPreview";
import { useNavigationState } from "./navigation/useNavigationState";

export const NAVIGATION_CONFIG = {
  CONTINUOUS_INTERVAL: 100,
  LONG_PRESS_THRESHOLD: 500,
} as const;

export type NavigationIndices = {
  runIndex: number;
  taskIndex: number;
};

type Props = {
  flatNodes: Array<GridTask>;
  isGridFocused: boolean;
  runs: Array<RunWithDuration>;
};

export type GridNavigationReturn = {
  currentIndices: NavigationIndices;
  isNavigating: boolean;
  navigateToPosition: (indices: NavigationIndices) => void;
  navigationCalculator: NavigationCalculator;
};

export const useGridNavigation = ({ flatNodes, isGridFocused, runs }: Props): GridNavigationReturn => {
  const navigate = useNavigate();
  const { dagId = "", groupId = "", runId = "", taskId = "" } = useParams();
  
  const navigationCalculator = useMemo(
    () => new NavigationCalculator(flatNodes, runs, { groupId, runId, taskId }),
    [flatNodes, runs, groupId, taskId, runId]
  );

  const {
    navigationState,
    resetNavigationState,
    startContinuousMode,
  } = useNavigationState();

  const { applyPreviewEffect, clearPreviewEffect } = useNavigationPreviewEffect(
    runs,
    flatNodes
  );

  const navigateToPosition = useCallback((indices: NavigationIndices) => {
    const { isValid, run, task } = navigationCalculator.getNavigationTarget(indices);
    
    if (!isValid || !run || !task) {
      return;
    }

    const { search } = globalThis.location;
    const path = getTaskNavigationPath({
      dagId,
      isGroup: task.isGroup,
      isMapped: Boolean(task.is_mapped),
      runId: run.dag_run_id,
      taskId: task.id,
    });

    navigate({ pathname: path, search }, { replace: true });
  }, [navigate, dagId, navigationCalculator]);

  useNavigationKeyboard({
    applyPreviewEffect,
    clearPreviewEffect,
    config: NAVIGATION_CONFIG,
    groupId,
    isGridFocused,
    navigateToPosition,
    navigationCalculator,
    resetNavigationState,
    runId,
    startContinuousMode,
    taskId,
  });

  return {
    currentIndices: navigationCalculator.getCurrentIndices(),
    isNavigating: navigationState === 'continuous',
    navigateToPosition,
    navigationCalculator,
  };
};