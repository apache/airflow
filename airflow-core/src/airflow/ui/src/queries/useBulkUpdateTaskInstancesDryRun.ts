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
import { useQuery, type UseQueryOptions } from "@tanstack/react-query";

import { OpenAPI } from "openapi/requests/core/OpenAPI";
import { request as __request } from "openapi/requests/core/request";
import type {
  BulkBody_BulkTaskInstanceBody_,
  TaskInstanceCollectionResponse,
} from "openapi/requests/types.gen";

type Props<TData, TError> = {
  dagId: string;
  dagRunId: string;
  options?: Omit<UseQueryOptions<TData, TError>, "queryFn" | "queryKey">;
  requestBody?: BulkBody_BulkTaskInstanceBody_;
};

export const useBulkUpdateTaskInstancesDryRunKey = "bulkUpdateTaskInstancesDryRun";

export const useBulkUpdateTaskInstancesDryRun = <TData = TaskInstanceCollectionResponse, TError = unknown>({
  dagId,
  dagRunId,
  options,
  requestBody,
}: Props<TData, TError>) =>
  useQuery<TData, TError>({
    ...options,
    queryFn: () =>
      __request(OpenAPI, {
        body: requestBody ?? { actions: [] },
        errors: {
          401: "Unauthorized",
          403: "Forbidden",
          422: "Validation Error",
        },
        mediaType: "application/json",
        method: "PATCH",
        path: {
          dag_id: dagId,
          dag_run_id: dagRunId,
        },
        query: {
          dry_run: true,
        },
        url: "/api/v2/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances",
      }) as Promise<TData>,
    queryKey: [useBulkUpdateTaskInstancesDryRunKey, dagId, dagRunId, requestBody],
  });
