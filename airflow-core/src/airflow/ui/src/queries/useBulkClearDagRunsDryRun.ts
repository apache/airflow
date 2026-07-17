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

import { DagRunService } from "openapi/requests/services.gen";
import type {
  DAGRunResponse,
  TaskInstanceCollectionResponse,
  TaskInstanceResponse,
} from "openapi/requests/types.gen";

type Options = {
  onlyFailed: boolean;
  onlyNew: boolean;
};

const EMPTY: TaskInstanceCollectionResponse = { task_instances: [], total_entries: 0 };

export const useBulkClearDagRunsDryRunKey = "bulkClearDagRunsDryRun";

export const useBulkClearDagRunsDryRun = (
  enabled: boolean,
  selectedDagRuns: Array<DAGRunResponse>,
  options: Options,
) => {
  const { data: response, isFetching } = useQuery({
    enabled: enabled && selectedDagRuns.length > 0,
    queryFn: () =>
      DagRunService.clearDagRuns({
        dagId: "~",
        requestBody: {
          dag_runs: selectedDagRuns.map((dagRun) => ({
            dag_id: dagRun.dag_id,
            dag_run_id: dagRun.dag_run_id,
          })),
          dry_run: true,
          only_failed: options.onlyFailed,
          only_new: options.onlyNew,
        },
      }),
    queryKey: [
      useBulkClearDagRunsDryRunKey,
      selectedDagRuns.map((dagRun) => `${dagRun.dag_id}.${dagRun.dag_run_id}`).sort(),
      { only_failed: options.onlyFailed, only_new: options.onlyNew },
    ],
    refetchOnMount: "always",
  });

  // ``clearDagRuns`` returns a union; ``dry_run`` always yields the task-instance
  // collection arm, so narrow on its shape. ``only_new=true`` yields
  // ``NewTaskResponse`` placeholders that render in the same affected-tasks table.
  const data: TaskInstanceCollectionResponse =
    response && "task_instances" in response
      ? {
          task_instances: response.task_instances as Array<TaskInstanceResponse>,
          total_entries: response.total_entries,
        }
      : EMPTY;

  return { data, isFetching };
};
