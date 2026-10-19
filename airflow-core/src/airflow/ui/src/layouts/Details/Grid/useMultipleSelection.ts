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
import { useState, useCallback, useMemo } from "react";

import type { GridTask } from "./utils";
import type { LightGridTaskInstanceSummary } from "openapi/types.gen";

export type SelectedTaskInstance = {
  taskId: string;
  runId: string;
  mapIndex?: number | null;
};

/**
 * Custom hook for managing multiple task instance selection in the Grid View.
 * Supports:
 * - Ctrl/Cmd + Click: Add/remove individual task instances
 * - Shift + Click: Select range between two task instances
 * - Click without modifiers: Clear selection and select single task instance
 */
export const useMultipleSelection = (
  flatNodes: Array<GridTask>,
  runIds: Array<string>,
  getTaskInstance: (taskId: string, runId: string) => LightGridTaskInstanceSummary | undefined,
) => {
  const [selectedTaskInstances, setSelectedTaskInstances] = useState<Array<SelectedTaskInstance>>([]);
  const [anchorSelection, setAnchorSelection] = useState<SelectedTaskInstance | null>(null);

  // Check if a task instance is selected
  const isSelected = useCallback(
    (taskId: string, runId: string) => {
      return selectedTaskInstances.some(
        (ti) => ti.taskId === taskId && ti.runId === runId,
      );
    },
    [selectedTaskInstances],
  );

  // Check if a column (run) has any selected task instances
  const isColumnSelected = useCallback(
    (runId: string) => {
      return selectedTaskInstances.some((ti) => ti.runId === runId);
    },
    [selectedTaskInstances],
  );

  // Get all selected task instances for a specific run
  const getSelectedForRun = useCallback(
    (runId: string) => {
      return selectedTaskInstances.filter((ti) => ti.runId === runId);
    },
    [selectedTaskInstances],
  );

  // Clear all selections
  const clearSelection = useCallback(() => {
    setSelectedTaskInstances([]);
    setAnchorSelection(null);
  }, []);

  // Add or remove a single task instance (Ctrl/Cmd + Click)
  const toggleTaskInstance = useCallback(
    (taskId: string, runId: string, mapIndex?: number | null) => {
      setSelectedTaskInstances((prev) => {
        const exists = prev.some((ti) => ti.taskId === taskId && ti.runId === runId);
        if (exists) {
          return prev.filter((ti) => !(ti.taskId === taskId && ti.runId === runId));
        }
        return [...prev, { taskId, runId, mapIndex }];
      });
    },
    [],
  );

  // Select a range of task instances (Shift + Click)
  const selectRange = useCallback(
    (taskId: string, runId: string, mapIndex?: number | null) => {
      if (!anchorSelection) {
        // No anchor, just add this one
        toggleTaskInstance(taskId, runId, mapIndex);
        setAnchorSelection({ taskId, runId, mapIndex });
        return;
      }

      // Find indices in the flattened grid
      const anchorTaskIndex = flatNodes.findIndex((n) => n.id === anchorSelection.taskId);
      const targetTaskIndex = flatNodes.findIndex((n) => n.id === taskId);
      const anchorRunIndex = runIds.indexOf(anchorSelection.runId);
      const targetRunIndex = runIds.indexOf(runId);

      if (anchorTaskIndex === -1 || targetTaskIndex === -1 || anchorRunIndex === -1 || targetRunIndex === -1) {
        return;
      }

      const minTaskIndex = Math.min(anchorTaskIndex, targetTaskIndex);
      const maxTaskIndex = Math.max(anchorTaskIndex, targetTaskIndex);
      const minRunIndex = Math.min(anchorRunIndex, targetRunIndex);
      const maxRunIndex = Math.max(anchorRunIndex, targetRunIndex);

      const newSelection: Array<SelectedTaskInstance> = [];

      for (let taskIdx = minTaskIndex; taskIdx <= maxTaskIndex; taskIdx++) {
        const node = flatNodes[taskIdx];
        if (!node) continue;

        for (let runIdx = minRunIndex; runIdx <= maxRunIndex; runIdx++) {
          const currentRunId = runIds[runIdx];
          if (!currentRunId) continue;
          const taskInstance = getTaskInstance(node.id, currentRunId);
          if (taskInstance) {
            newSelection.push({
              taskId: node.id,
              runId: currentRunId,
              mapIndex: taskInstance.latest_region_index ?? null,
            });
          }
        }
      }

      // Merge with existing selection (excluding the range area to avoid duplicates)
      setSelectedTaskInstances((prev) => {
        const existingOutsideRange = prev.filter((ti) => {
          const taskIdx = flatNodes.findIndex((n) => n.id === ti.taskId);
          const runIdx = runIds.indexOf(ti.runId);
          return (
            taskIdx < minTaskIndex ||
            taskIdx > maxTaskIndex ||
            runIdx < minRunIndex ||
            runIdx > maxRunIndex
          );
        });
        return [...existingOutsideRange, ...newSelection];
      });
    },
    [anchorSelection, flatNodes, runIds, getTaskInstance, toggleTaskInstance],
  );

  // Main click handler that handles all selection modes
  const handleClick = useCallback(
    (
      taskId: string,
      runId: string,
      event: React.MouseEvent<HTMLDivElement, MouseEvent>,
      mapIndex?: number | null,
    ) => {
      const isCtrlOrMeta = event.ctrlKey || event.metaKey;
      const isShift = event.shiftKey;

      if (isCtrlOrMeta) {
        // Ctrl/Cmd + Click: Toggle individual selection
        toggleTaskInstance(taskId, runId, mapIndex);
        if (!isSelected(taskId, runId)) {
          setAnchorSelection({ taskId, runId, mapIndex });
        }
      } else if (isShift) {
        // Shift + Click: Range selection
        selectRange(taskId, runId, mapIndex);
      } else {
        // Simple click: Clear and select single
        clearSelection();
        toggleTaskInstance(taskId, runId, mapIndex);
        setAnchorSelection({ taskId, runId, mapIndex });
      }
    },
    [toggleTaskInstance, selectRange, clearSelection, isSelected],
  );

  // Get all selected task instances with their full details
  const getSelectedTaskInstancesDetails = useCallback(() => {
    return selectedTaskInstances
      .map((sel) => {
        const instance = getTaskInstance(sel.taskId, sel.runId);
        return instance ? { ...sel, ...instance } : sel;
      })
      .filter((ti): ti is SelectedTaskInstance & LightGridTaskInstanceSummary => "state" in ti);
  }, [selectedTaskInstances, getTaskInstance]);

  // Memoized values to prevent unnecessary re-renders
  const selectionCount = useMemo(() => selectedTaskInstances.length, [selectedTaskInstances]);
  const hasSelection = useMemo(() => selectedTaskInstances.length > 0, [selectedTaskInstances]);

  return {
    selectedTaskInstances,
    selectionCount,
    hasSelection,
    isSelected,
    isColumnSelected,
    getSelectedForRun,
    getSelectedTaskInstancesDetails,
    handleClick,
    clearSelection,
    toggleTaskInstance,
    selectRange,
  };
};

export default useMultipleSelection;