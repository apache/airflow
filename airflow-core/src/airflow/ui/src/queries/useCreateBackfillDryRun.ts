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

import { BackfillService } from "openapi/requests/services.gen";
import type { CreateBackfillDryRunData, DryRunBackfillCollectionResponse } from "openapi/requests/types.gen";

type Props<TData, TError> = {
  options?: Omit<UseQueryOptions<TData, TError>, "queryFn" | "queryKey">;
  requestBody: CreateBackfillDryRunData;
};

const useCreateBackfillDryRunKey = "useCreateBackfillDryRunKey";

const validateHeaderName = (requestBody: CreateBackfillDryRunData) => {
  if (requestBody.requestBody.from_date === "" || requestBody.requestBody.to_date === "") {
    return false;
  }
  const dataIntervalStart = new Date(requestBody.requestBody.from_date);
  const dataIntervalEnd = new Date(requestBody.requestBody.to_date);

  if (dataIntervalStart > dataIntervalEnd) {
    return false;
  }
  if (Number(requestBody.requestBody.max_active_runs ?? 0) < 1) {
    return false;
  }

  return true;
};

export const useCreateBackfillDryRun = <TData = DryRunBackfillCollectionResponse, TError = unknown>({
  options,
  requestBody,
}: Props<TData, TError>) => {
  const { max_active_runs: maxActiveRuns, ...bodyForKey } = requestBody.requestBody;

  return useQuery<TData, TError>({
    ...options,
    enabled: validateHeaderName(requestBody),
    queryFn: () =>
      BackfillService.createBackfillDryRun({
        ...requestBody,
      }) as TData,
    queryKey: [useCreateBackfillDryRunKey, bodyForKey],
  });
};
