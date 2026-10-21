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
import { useQuery } from "@tanstack/react-query";

import { useTaskInstanceServiceGetTaskInstancesKey } from "openapi/queries";
import { TaskInstanceService } from "openapi/requests/services.gen";
import type { TaskInstanceResponse } from "openapi/requests/types.gen";

import type { GridSelection } from "src/context/gridSelection";

import { getCellKey } from "./useGridSelection";

const PAGE_LIMIT = 100;

/**
 * Load the task instances behind the selected grid cells. A mapped task's cell stands for all of
 * its map indexes, as it does when the cell is clicked.
 */
export const useGridSelectedTaskInstances = (dagId: string, selection: GridSelection) => {
  const cells = [...selection.values()];
  const runIds = [...new Set(cells.map((cell) => cell.runId))].sort();
  const taskIds = [...new Set(cells.map((cell) => cell.taskId))].sort();

  return useQuery({
    enabled: selection.size > 0,
    queryFn: async () => {
      const taskInstances: Array<TaskInstanceResponse> = [];
      let done = false;

      // The request asks for every selected task in every selected run, so a sparse selection
      // returns cells that were not picked and they are dropped below.
      while (!done) {
        // eslint-disable-next-line no-await-in-loop
        const page = await TaskInstanceService.getTaskInstancesBatch({
          dagId: "~",
          dagRunId: "~",
          requestBody: {
            dag_ids: [dagId],
            dag_run_ids: runIds,
            order_by: "id",
            page_limit: PAGE_LIMIT,
            page_offset: taskInstances.length,
            task_ids: taskIds,
          },
        });

        taskInstances.push(...page.task_instances);
        done = page.task_instances.length === 0 || taskInstances.length >= (page.total_entries ?? Infinity);
      }

      return taskInstances.filter((ti) => selection.has(getCellKey(ti.dag_run_id, ti.task_id)));
    },
    // Under the task instances key so the bulk actions' invalidation refreshes the selection too.
    queryKey: [
      useTaskInstanceServiceGetTaskInstancesKey,
      "gridSelection",
      dagId,
      [...selection.keys()].sort(),
    ],
  });
};
