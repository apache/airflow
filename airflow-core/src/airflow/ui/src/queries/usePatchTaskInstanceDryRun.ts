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

import { TaskInstanceService } from "openapi/requests/services.gen";
import type { PatchTaskInstanceBody, PatchTaskInstanceDryRunResponse } from "openapi/requests/types.gen";

type Props<TData, TError> = {
  dagId: string;
  dagRunId: string;
  mapIndex: number;
  options?: Omit<UseQueryOptions<TData, TError>, "queryFn" | "queryKey">;
  requestBody: PatchTaskInstanceBody;
  taskId: string;
};

export const usePatchTaskInstanceDryRunKey = "patchTaskInstanceDryRun";

export const usePatchTaskInstanceDryRun = <TData = PatchTaskInstanceDryRunResponse, TError = unknown>({
  dagId,
  dagRunId,
  mapIndex,
  options,
  requestBody,
  taskId,
}: Props<TData, TError>) =>
  useQuery<TData, TError>({
    ...options,
    queryFn: () =>
      TaskInstanceService.patchTaskInstanceDryRun({
        dagId,
        dagRunId,
        mapIndex,
        requestBody,
        taskId,
      }) as TData,
    queryKey: [
      usePatchTaskInstanceDryRunKey,
      dagId,
      dagRunId,
      {
        include_downstream: requestBody.include_downstream,
        include_future: requestBody.include_future,
        include_past: requestBody.include_past,
        include_upstream: requestBody.include_upstream,
        mapIndex,
        new_state: requestBody.new_state,
        taskId,
      },
    ],
  });
