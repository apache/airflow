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
import type { TaskInstanceCollectionResponse, TaskInstanceResponse } from "openapi/requests/types.gen";

type Options = {
  includeDownstream: boolean;
  includeFuture: boolean;
  includeOnlyFailed: boolean;
  includePast: boolean;
  includeUpstream: boolean;
};

const EMPTY: TaskInstanceCollectionResponse = { task_instances: [], total_entries: 0 };

export const useBulkClearDryRunKey = "bulkClearDryRun";

export const useBulkClearDryRun = (
  enabled: boolean,
  selectedTaskInstances: Array<TaskInstanceResponse>,
  options: Options,
) => {
  const byDagRun = useMemo(() => {
    const groups = new Map<string, { dagId: string; dagRunId: string; tis: Array<TaskInstanceResponse> }>();

    for (const ti of selectedTaskInstances) {
      const key = `${ti.dag_id}::${ti.dag_run_id}`;
      const group = groups.get(key) ?? { dagId: ti.dag_id, dagRunId: ti.dag_run_id, tis: [] };

      group.tis.push(ti);
      groups.set(key, group);
    }

    return [...groups.values()];
  }, [selectedTaskInstances]);

  const results = useQueries({
    queries: byDagRun.map(({ dagId, dagRunId, tis }) => ({
      enabled,
      queryFn: () =>
        TaskInstanceService.postClearTaskInstances({
          dagId,
          requestBody: {
            dag_run_id: dagRunId,
            dry_run: true,
            include_downstream: options.includeDownstream,
            include_future: options.includeFuture,
            include_past: options.includePast,
            include_upstream: options.includeUpstream,
            only_failed: options.includeOnlyFailed,
            task_ids: tis.map((ti) =>
              ti.map_index >= 0 ? ([ti.task_id, ti.map_index] as [string, number]) : ti.task_id,
            ),
          },
        }),
      queryKey: [
        useBulkClearDryRunKey,
        dagId,
        dagRunId,
        {
          include_downstream: options.includeDownstream,
          include_future: options.includeFuture,
          include_only_failed: options.includeOnlyFailed,
          include_past: options.includePast,
          include_upstream: options.includeUpstream,
          task_ids: tis.map((ti) => `${ti.task_id}:${ti.map_index}`),
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
