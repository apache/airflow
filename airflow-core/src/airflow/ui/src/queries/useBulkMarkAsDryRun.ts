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
import { useMemo } from "react";

import { TaskInstanceService } from "openapi/requests/services.gen";
import type {
  TaskInstanceCollectionResponse,
  TaskInstanceResponse,
  TaskInstanceState,
} from "openapi/requests/types.gen";

type Options = {
  includeDownstream: boolean;
  includeFuture: boolean;
  includePast: boolean;
  includeUpstream: boolean;
};

const EMPTY: TaskInstanceCollectionResponse = { task_instances: [], total_entries: 0 };

export const useBulkMarkAsDryRunKey = "bulkMarkAsDryRun";

export const useBulkMarkAsDryRun = (
  enabled: boolean,
  {
    options,
    selectedTaskInstances,
    targetState,
  }: {
    options: Options;
    selectedTaskInstances: Array<TaskInstanceResponse>;
    targetState: TaskInstanceState;
  },
) => {
  const affectedInstances = useMemo(
    () => selectedTaskInstances.filter((ti) => ti.state !== targetState),
    [selectedTaskInstances, targetState],
  );

  const results = useQueries({
    queries: affectedInstances.map((ti) => ({
      enabled,
      queryFn: () =>
        TaskInstanceService.patchTaskInstanceDryRun({
          dagId: ti.dag_id,
          dagRunId: ti.dag_run_id,
          mapIndex: ti.map_index,
          requestBody: {
            include_downstream: options.includeDownstream,
            include_future: options.includeFuture,
            include_past: options.includePast,
            include_upstream: options.includeUpstream,
            new_state: targetState,
          },
          taskId: ti.task_id,
        }),
      queryKey: [
        useBulkMarkAsDryRunKey,
        ti.dag_id,
        ti.dag_run_id,
        ti.task_id,
        ti.map_index,
        {
          include_downstream: options.includeDownstream,
          include_future: options.includeFuture,
          include_past: options.includePast,
          include_upstream: options.includeUpstream,
          new_state: targetState,
        },
      ],
      refetchOnMount: "always" as const,
    })),
  });

  const isFetching = results.some((result) => result.isFetching);

  const data = useMemo<TaskInstanceCollectionResponse>(() => {
    const seen = new Set<string>();
    const merged: Array<TaskInstanceResponse> = [];

    for (const result of results) {
      for (const ti of result.data?.task_instances ?? []) {
        const key = `${ti.dag_id}:${ti.dag_run_id}:${ti.task_id}:${ti.map_index}`;

        if (!seen.has(key)) {
          seen.add(key);
          merged.push(ti);
        }
      }
    }

    return merged.length === 0 ? EMPTY : { task_instances: merged, total_entries: merged.length };
  }, [results]);

  return { data, isFetching };
};
