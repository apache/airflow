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
  isPartitioned: boolean;
  options?: Omit<UseQueryOptions<TData, TError>, "queryFn" | "queryKey">;
  requestBody: CreateBackfillDryRunData;
};

const useCreateBackfillDryRunKey = "useCreateBackfillDryRunKey";

const validateDryRun = (requestBody: CreateBackfillDryRunData, isPartitioned: boolean) => {
  if (Number(requestBody.requestBody.max_active_runs ?? 0) < 1) {
    return false;
  }

  if (isPartitioned) {
    const start = requestBody.requestBody.partition_date_start;
    const end = requestBody.requestBody.partition_date_end;

    if (start === "" || start === null || start === undefined || end === "" || end === null || end === undefined) {
      return false;
    }

    const partitionStart = new Date(start);
    const partitionEnd = new Date(end);

    if (partitionStart > partitionEnd) {
      return false;
    }
  } else {
    const from = requestBody.requestBody.from_date;
    const to = requestBody.requestBody.to_date;

    if (from === "" || from === null || from === undefined || to === "" || to === null || to === undefined) {
      return false;
    }

    const dataIntervalStart = new Date(from);
    const dataIntervalEnd = new Date(to);

    if (dataIntervalStart > dataIntervalEnd) {
      return false;
    }
  }

  return true;
};

export const useCreateBackfillDryRun = <TData = DryRunBackfillCollectionResponse, TError = unknown>({
  isPartitioned,
  options,
  requestBody,
}: Props<TData, TError>) => {
  const { max_active_runs: maxActiveRuns, ...bodyForKey } = requestBody.requestBody;

  return useQuery<TData, TError>({
    ...options,
    enabled: validateDryRun(requestBody, isPartitioned),
    queryFn: () =>
      BackfillService.createBackfillDryRun({
        ...requestBody,
      }) as TData,
    queryKey: [useCreateBackfillDryRunKey, bodyForKey],
  });
};
