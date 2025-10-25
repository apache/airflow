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
import { useEffect, useRef } from "react";
import { useLocalStorage } from "usehooks-ts";

import type { FilterMode } from "src/layouts/Details/Graph/LineageFilter";

type UseLineageFilterProps = {
  readonly dagId: string;
  readonly dagView: "graph" | "grid";
  readonly taskId?: string;
};

type UseLineageFilterReturn = {
  readonly handleClearLineageFilter: () => void;
  readonly handleLineageFilterModeChange: (mode: FilterMode) => void;
  readonly lineageFilterMode: FilterMode;
  readonly lineageFilterRoot: string | undefined;
  readonly setLineageFilterRoot: (root: string | undefined) => void;
};

/**
 * Custom hook to manage lineage filter state for the DAG graph view.
 *
 * This hook handles:
 * - Persisting filter state in localStorage
 * - Clearing filter on initial mount when no task is selected
 * - Updating filter root when switching between views
 * - Managing filter state when tasks are clicked
 */
export const useLineageFilter = ({
  dagId,
  dagView,
  taskId,
}: UseLineageFilterProps): UseLineageFilterReturn => {
  const prevDagViewRef = useRef(dagView);
  const prevTaskIdRef = useRef<string | undefined>(taskId);
  const isManualFilterChangeRef = useRef(false);

  // Use localStorage for lineage filter state so it persists across navigation
  // and updates when tasks are selected from grid or other views
  // When updated in other views, the filter is removed.
  const [lineageFilterMode, setLineageFilterMode] = useLocalStorage<FilterMode>(
    `lineage_filter_mode-${dagId}`,
    "none",
  );
  const [lineageFilterRoot, setLineageFilterRoot] = useLocalStorage<string | undefined>(
    `lineage_filter_root-${dagId}`,
    undefined,
  );

  // Use refs to track current filter values without causing effect re-runs
  const lineageFilterModeRef = useRef(lineageFilterMode);
  const lineageFilterRootRef = useRef(lineageFilterRoot);
  const isInitialMountRef = useRef(true);

  useEffect(() => {
    lineageFilterModeRef.current = lineageFilterMode;
    lineageFilterRootRef.current = lineageFilterRoot;
  }, [lineageFilterMode, lineageFilterRoot]);

  // Clear filter on initial mount if no task is selected
  useEffect(() => {
    if (isInitialMountRef.current) {
      isInitialMountRef.current = false;

      // If there's a filter saved in localStorage but no task is selected on mount,
      // clear the filter to prevent showing filtered view without context
      if (taskId === undefined && lineageFilterMode !== "none") {
        setLineageFilterMode("none");
      }
    }
  }, [taskId, lineageFilterMode, setLineageFilterMode]);

  const handleLineageFilterModeChange = (mode: FilterMode) => {
    isManualFilterChangeRef.current = true;
    setLineageFilterMode(mode);
    if (mode !== "none" && taskId !== undefined && taskId !== lineageFilterRoot) {
      setLineageFilterRoot(taskId);
    }
  };

  const handleClearLineageFilter = () => {
    isManualFilterChangeRef.current = true;
    setLineageFilterMode("none");
  };

  useEffect(() => {
    if (isManualFilterChangeRef.current) {
      isManualFilterChangeRef.current = false;
      prevDagViewRef.current = dagView;
      prevTaskIdRef.current = taskId;

      return;
    }

    const isComingFromOtherView = prevDagViewRef.current !== "graph" && dagView === "graph";
    const isAlreadyInGraphView = prevDagViewRef.current === "graph" && dagView === "graph";
    const hasActiveFilter = lineageFilterModeRef.current !== "none";
    const currentRoot = lineageFilterRootRef.current;

    // Only update state if taskId or dagView actually changed
    const taskIdChanged = prevTaskIdRef.current !== taskId;
    const dagViewChanged = prevDagViewRef.current !== dagView;

    if (taskId !== undefined && dagView === "graph" && (taskIdChanged || dagViewChanged)) {
      if (taskId !== currentRoot && isComingFromOtherView) {
        setLineageFilterRoot(taskId);
        setLineageFilterMode("none");
      } else if (taskId !== currentRoot && isAlreadyInGraphView && !hasActiveFilter) {
        // Only update the root if already in graph view AND no filter is active
        // When a filter is active, clicking a different task won't change the root
        // The root only changes when user manually selects a filter from the menu
        setLineageFilterRoot(taskId);
      }
    }

    prevDagViewRef.current = dagView;
    prevTaskIdRef.current = taskId;
  }, [taskId, dagView, setLineageFilterRoot, setLineageFilterMode]);

  return {
    handleClearLineageFilter,
    handleLineageFilterModeChange,
    lineageFilterMode,
    lineageFilterRoot,
    setLineageFilterRoot,
  };
};
