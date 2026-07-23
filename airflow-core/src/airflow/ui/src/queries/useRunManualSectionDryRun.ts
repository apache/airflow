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
import type { RunManualSectionBody, RunManualSectionResponse } from "openapi/requests/types.gen";

type Props<TData, TError> = {
  dagId: string;
  dagRunId: string;
  options?: Omit<UseQueryOptions<TData, TError>, "queryFn" | "queryKey">;
  requestBody: RunManualSectionBody;
  taskId: string;
};

export const useRunManualSectionDryRunKey = "runManualSectionDryRun";

export const useRunManualSectionDryRun = <TData = RunManualSectionResponse, TError = unknown>({
  dagId,
  dagRunId,
  options,
  requestBody,
  taskId,
}: Props<TData, TError>) =>
  useQuery<TData, TError>({
    ...options,
    queryFn: () =>
      TaskInstanceService.runManualSection({
        dagId,
        dagRunId,
        requestBody: {
          dry_run: true,
          ...requestBody,
        },
        taskId,
      }) as TData,
    queryKey: [useRunManualSectionDryRunKey, dagId, dagRunId, taskId, requestBody],
  });
