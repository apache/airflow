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

import type { FilterMode } from "src/layouts/Details/LineageFilter";

type UseLineageFilterProps = {
  readonly dagId: string;
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
 * Custom hook to manage lineage filter state for the DAG views (Graph and Grid).
 *
 * This hook handles:
 * - Persisting filter state in localStorage
 * - Clearing filter on initial mount when no task is selected
 * - Managing filter state when tasks are clicked
 */
export const useLineageFilter = ({ dagId, taskId }: UseLineageFilterProps): UseLineageFilterReturn => {
  // Use localStorage for lineage filter state so it persists across navigation
  const [lineageFilterMode, setLineageFilterMode] = useLocalStorage<FilterMode>(
    `lineage_filter_mode-${dagId}`,
    "none",
  );
  const [lineageFilterRoot, setLineageFilterRoot] = useLocalStorage<string | undefined>(
    `lineage_filter_root-${dagId}`,
    undefined,
  );

  const isInitialMountRef = useRef(true);

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
    setLineageFilterMode(mode);
    if (mode !== "none" && taskId !== undefined && taskId !== lineageFilterRoot) {
      setLineageFilterRoot(taskId);
    }
  };

  const handleClearLineageFilter = () => {
    setLineageFilterMode("none");
  };

  return {
    handleClearLineageFilter,
    handleLineageFilterModeChange,
    lineageFilterMode,
    lineageFilterRoot,
    setLineageFilterRoot,
  };
};
