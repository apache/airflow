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
import { useQueries } from "@tanstack/react-query";

import { DagRunService } from "openapi/requests/services.gen";
import type {
  ClearTaskInstanceCollectionResponse,
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
  const results = useQueries({
    queries: selectedDagRuns.map((dagRun) => ({
      enabled,
      queryFn: () =>
        DagRunService.clearDagRun({
          dagId: dagRun.dag_id,
          dagRunId: dagRun.dag_run_id,
          requestBody: {
            dry_run: true,
            only_failed: options.onlyFailed,
            only_new: options.onlyNew,
          },
        }) as Promise<ClearTaskInstanceCollectionResponse>,
      queryKey: [
        useBulkClearDagRunsDryRunKey,
        dagRun.dag_id,
        dagRun.dag_run_id,
        { only_failed: options.onlyFailed, only_new: options.onlyNew },
      ],
      refetchOnMount: "always" as const,
    })),
  });

  const isFetching = results.some((result) => result.isFetching);
  // Each per-run call is scoped to a distinct ``(dag_id, dag_run_id)`` so the
  // concatenated array can't contain duplicates; the response is also
  // homogeneous (``only_new=true`` yields ``NewTaskResponse`` placeholders,
  // ``false`` yields real ``TaskInstanceResponse``), so the cast is safe even
  // though the OpenAPI type widens to a union.
  const taskInstances = results.flatMap((result) => result.data?.task_instances ?? []);
  const data: TaskInstanceCollectionResponse =
    taskInstances.length === 0
      ? EMPTY
      : {
          task_instances: taskInstances as Array<TaskInstanceResponse>,
          total_entries: taskInstances.length,
        };

  return { data, isFetching };
};
