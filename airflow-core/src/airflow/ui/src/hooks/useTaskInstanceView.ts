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
import { useParams, useSearchParams } from "react-router-dom";

import {
  useTaskInstanceServiceGetMappedTaskInstance,
  useTaskInstanceServiceGetTaskInstanceTryDetails,
} from "openapi/queries";

import { SearchParamsKeys } from "src/constants/searchParams";
import { useTaskInstanceCoordinates } from "src/hooks/useTaskInstanceCoordinates";
import { isStatePending, useAutoRefresh } from "src/utils";

export const isExactTryView = (searchParams: URLSearchParams) =>
  searchParams.has(SearchParamsKeys.TRY_NUMBER) &&
  searchParams.has(SearchParamsKeys.REGION_ID) &&
  searchParams.has(SearchParamsKeys.REGION_INDEX);

export const useTaskInstanceView = () => {
  const { dagId = "", mapIndex = "-1", runId = "", taskId = "" } = useParams();
  const [searchParams] = useSearchParams();
  const coordinates = useTaskInstanceCoordinates();
  const tryParameter = searchParams.get(SearchParamsKeys.TRY_NUMBER);
  const exactTry = isExactTryView(searchParams);
  const refetchInterval = useAutoRefresh({ dagId });
  const params = { ...coordinates, dagId, dagRunId: runId, mapIndex: Number(mapIndex), taskId };
  const live = useTaskInstanceServiceGetMappedTaskInstance(params, undefined, {
    enabled: !Number.isNaN(params.mapIndex),
    refetchInterval: (query) => isStatePending(query.state.data?.state) && refetchInterval,
    ...(exactTry ? { retry: false } : {}),
    staleTime: 0,
  });
  const history = useTaskInstanceServiceGetTaskInstanceTryDetails(
    { ...params, taskTryNumber: Number(tryParameter) },
    undefined,
    { enabled: exactTry },
  );
  const historical =
    exactTry &&
    history.data !== undefined &&
    !live.isPending &&
    (live.data?.id !== history.data.id || live.data.try_number !== history.data.try_number);

  return {
    error: exactTry ? (history.error ?? (history.data === undefined ? live.error : null)) : live.error,
    historical,
    historicalTaskInstance: historical ? history.data : undefined,
    isLoading: exactTry ? history.isLoading || live.isLoading : live.isLoading,
    liveTaskInstance: historical || (exactTry && history.data === undefined) ? undefined : live.data,
    taskInstance: historical ? history.data : exactTry && history.data === undefined ? undefined : live.data,
  };
};
