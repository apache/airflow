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
import type { MouseEvent } from "react";
import { useCallback, useEffect, useRef, useState } from "react";

import { useParams } from "react-router-dom";

import type { GridTISummaries, LightGridTaskInstanceSummary } from "openapi/requests/types.gen";

import type { GridTask } from "./utils";

export type GridCell = {
  readonly runId: string;
  readonly taskId: string;
};

export type GridSelection = ReadonlyMap<string, GridCell>;

export const getCellKey = (runId: string, taskId: string) => JSON.stringify([runId, taskId]);

// A looped task's cell aggregates every pass while its link opens only the newest one, so a bulk
// action on it would be ambiguous. Groups and cells without a task instance have nothing to act on.
export const isSelectableCell = (node: GridTask, taskInstance: LightGridTaskInstanceSummary | undefined) =>
  !node.isGroup &&
  taskInstance?.dag_version_number !== null &&
  taskInstance?.dag_version_number !== undefined &&
  (taskInstance.latest_region_id === null || taskInstance.latest_region_id === undefined);

type GridData = {
  readonly flatNodes: Array<GridTask>;
  readonly runIds: Array<string>;
  readonly summariesByRunId: ReadonlyMap<string, GridTISummaries>;
};

const findTaskInstance = (data: GridData, runId: string, taskId: string) =>
  data.summariesByRunId.get(runId)?.task_instances.find((ti) => ti.task_id === taskId);

export const buildRangeSelection = (data: GridData, from: GridCell, to: GridCell): Map<string, GridCell> => {
  const rows = [from.taskId, to.taskId].map((taskId) =>
    data.flatNodes.findIndex((node) => node.id === taskId),
  );
  const columns = [from.runId, to.runId].map((runId) => data.runIds.indexOf(runId));
  const selection = new Map<string, GridCell>();

  if ([...rows, ...columns].includes(-1)) {
    return selection;
  }

  for (const node of data.flatNodes.slice(Math.min(...rows), Math.max(...rows) + 1)) {
    for (const runId of data.runIds.slice(Math.min(...columns), Math.max(...columns) + 1)) {
      if (isSelectableCell(node, findTaskInstance(data, runId, node.id))) {
        selection.set(getCellKey(runId, node.id), { runId, taskId: node.id });
      }
    }
  }

  return selection;
};

/**
 * Select task instance cells in the grid: Ctrl/Cmd + click toggles a cell, Shift + click selects
 * the rectangle between the last toggled (or currently opened) cell and the clicked one, and a
 * plain click clears the selection and navigates as before.
 */
export const useGridSelection = (data: GridData) => {
  const { dagId = "", runId: openedRunId, taskId: openedTaskId } = useParams();
  const [selection, setSelection] = useState<{ cells: GridSelection; dagId: string }>({
    cells: new Map(),
    dagId,
  });
  const anchorRef = useRef<GridCell | undefined>(undefined);
  // The click handler is passed to every run column; reading the grid data through a ref keeps it
  // stable while the TI summaries stream in, so a summary update does not re-render every column.
  const dataRef = useRef(data);

  useEffect(() => {
    dataRef.current = data;
  });

  const cells = selection.dagId === dagId ? selection.cells : new Map<string, GridCell>();

  const clearSelection = useCallback(() => {
    anchorRef.current = undefined;
    setSelection({ cells: new Map(), dagId });
  }, [dagId]);

  const deselectCells = useCallback(
    (keys: Array<string>) =>
      setSelection((prev) => {
        const next = new Map(prev.cells);

        keys.forEach((key) => next.delete(key));

        return { cells: next, dagId: prev.dagId };
      }),
    [],
  );

  // Returns whether the click changed the selection, in which case it must not navigate.
  const handleSelectionClick = useCallback(
    (event: MouseEvent, cell: GridCell): boolean => {
      const isSelectable = ({ runId, taskId }: GridCell) => {
        const node = dataRef.current.flatNodes.find((flatNode) => flatNode.id === taskId);

        return node !== undefined && isSelectableCell(node, findTaskInstance(dataRef.current, runId, taskId));
      };
      const openedCell =
        openedRunId !== undefined && openedTaskId !== undefined
          ? { runId: openedRunId, taskId: openedTaskId }
          : undefined;

      if (event.shiftKey && isSelectable(cell)) {
        const anchor = anchorRef.current ?? openedCell ?? cell;

        anchorRef.current = anchor;
        setSelection({ cells: buildRangeSelection(dataRef.current, anchor, cell), dagId });

        return true;
      }

      if ((event.ctrlKey || event.metaKey) && isSelectable(cell)) {
        const key = getCellKey(cell.runId, cell.taskId);

        anchorRef.current = cell;
        setSelection((prev) => {
          const next = new Map(prev.dagId === dagId ? prev.cells : []);

          // The opened task instance is the starting point of a selection, like the active cell
          // of a spreadsheet.
          if (next.size === 0 && openedCell !== undefined && isSelectable(openedCell)) {
            const openedKey = getCellKey(openedCell.runId, openedCell.taskId);

            if (openedKey !== key) {
              next.set(openedKey, openedCell);
            }
          }

          if (next.has(key)) {
            next.delete(key);
          } else {
            next.set(key, cell);
          }

          return { cells: next, dagId };
        });

        return true;
      }

      if (!event.shiftKey && !event.ctrlKey && !event.metaKey) {
        clearSelection();
      }

      return false;
    },
    [clearSelection, dagId, openedRunId, openedTaskId],
  );

  return { clearSelection, deselectCells, handleSelectionClick, selection: cells };
};
