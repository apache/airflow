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
import { useQueries, useQuery, type UseQueryOptions } from "@tanstack/react-query";

import { TaskInstanceService } from "openapi/requests/services.gen";
import type {
  ClearTaskInstancesBody,
  PostClearTaskInstancesResponse,
  TaskInstanceCollectionResponse,
  TaskInstanceResponse,
} from "openapi/requests/types.gen";

type Props<TData, TError> = {
  dagId: string;
  options?: Omit<UseQueryOptions<TData, TError>, "queryFn" | "queryKey">;
  requestBody: ClearTaskInstancesBody;
};

export const useClearTaskInstancesDryRunKey = "clearTaskInstanceDryRun";

export const useClearTaskInstancesDryRun = <TData = PostClearTaskInstancesResponse, TError = unknown>({
  dagId,
  options,
  requestBody,
}: Props<TData, TError>) =>
  useQuery<TData, TError>({
    ...options,
    queryFn: () =>
      TaskInstanceService.postClearTaskInstances({
        dagId,
        requestBody: {
          dry_run: true,
          ...requestBody,
        },
      }) as TData,
    queryKey: [useClearTaskInstancesDryRunKey, dagId, requestBody],
  });

type DryRunRequest = {
  dagId: string;
  requestBody: ClearTaskInstancesBody;
};

const EMPTY_DRY_RUN: TaskInstanceCollectionResponse = { task_instances: [], total_entries: 0 };

/** Previews several clear requests at once (a selection can span runs and Dags) as one merged list. */
export const useClearTaskInstancesDryRuns = ({
  options,
  requests,
}: {
  options?: Omit<UseQueryOptions<PostClearTaskInstancesResponse>, "queryFn" | "queryKey">;
  requests: Array<DryRunRequest>;
}) =>
  useQueries({
    combine: (results) => {
      const merged = new Map<string, TaskInstanceResponse>();

      for (const result of results) {
        for (const ti of result.data?.task_instances ?? []) {
          merged.set(ti.id, ti);
        }
      }

      return {
        data:
          merged.size === 0
            ? EMPTY_DRY_RUN
            : { task_instances: [...merged.values()], total_entries: merged.size },
        error: results.find((result) => result.error !== null)?.error ?? null,
        isFetching: results.some((result) => result.isFetching),
      };
    },
    queries: requests.map(({ dagId, requestBody }) => ({
      ...options,
      queryFn: () =>
        TaskInstanceService.postClearTaskInstances({
          dagId,
          requestBody: {
            dry_run: true,
            ...requestBody,
          },
        }),
      queryKey: [useClearTaskInstancesDryRunKey, dagId, requestBody],
    })),
  });
