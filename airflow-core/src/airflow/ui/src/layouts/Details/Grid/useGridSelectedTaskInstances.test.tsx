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
import { renderHook, waitFor } from "@testing-library/react";
import { afterEach, expect, it, vi } from "vitest";

import { TaskInstanceService } from "openapi/requests";
import { CancelablePromise } from "openapi/requests/core/CancelablePromise";
import type { TaskInstanceResponse } from "openapi/requests/types.gen";

import type { GridCell } from "src/context/gridSelection";
import { Wrapper } from "src/utils/Wrapper";

import { useGridSelectedTaskInstances } from "./useGridSelectedTaskInstances";
import { getCellKey } from "./useGridSelection";

afterEach(() => vi.restoreAllMocks());

const taskInstance = (runId: string, taskId: string, mapIndex = -1) =>
  ({
    dag_id: "dag",
    dag_run_id: runId,
    id: `${runId}-${taskId}-${mapIndex}`,
    map_index: mapIndex,
    task_id: taskId,
  }) as TaskInstanceResponse;

const selectionOf = (cells: Array<GridCell>) =>
  new Map(cells.map((cell) => [getCellKey(cell.runId, cell.taskId), cell]));

it("loads every page and keeps only the selected cells, with all map indexes of a mapped task", async () => {
  // Selecting (r1, mapped) and (r2, plain) asks for both tasks in both runs.
  const rows = [
    ...Array.from({ length: 100 }, (_, mapIndex) => taskInstance("r1", "mapped", mapIndex)),
    taskInstance("r1", "plain"),
    taskInstance("r2", "mapped", 0),
    taskInstance("r2", "plain"),
  ];
  const batch = vi
    .spyOn(TaskInstanceService, "getTaskInstancesBatch")
    .mockImplementation(({ requestBody }) => {
      const offset = requestBody.page_offset ?? 0;

      return new CancelablePromise((resolve) =>
        resolve({ task_instances: rows.slice(offset, offset + 100), total_entries: rows.length }),
      );
    });

  const { result } = renderHook(
    () =>
      useGridSelectedTaskInstances(
        "dag",
        selectionOf([
          { runId: "r1", taskId: "mapped" },
          { runId: "r2", taskId: "plain" },
        ]),
      ),
    { wrapper: Wrapper },
  );

  await waitFor(() => expect(result.current.isSuccess).toBe(true));
  expect(batch).toHaveBeenCalledTimes(2);
  expect(batch.mock.calls[0]?.[0].requestBody).toMatchObject({
    dag_ids: ["dag"],
    dag_run_ids: ["r1", "r2"],
    page_offset: 0,
    task_ids: ["mapped", "plain"],
  });
  expect(batch.mock.calls[1]?.[0].requestBody.page_offset).toBe(100);
  expect(result.current.data?.map((ti) => ti.id)).toEqual([
    ...Array.from({ length: 100 }, (_, mapIndex) => `r1-mapped-${mapIndex}`),
    "r2-plain--1",
  ]);
});

it("does not load anything without a selection", () => {
  const batch = vi.spyOn(TaskInstanceService, "getTaskInstancesBatch");

  renderHook(() => useGridSelectedTaskInstances("dag", new Map()), { wrapper: Wrapper });

  expect(batch).not.toHaveBeenCalled();
});
