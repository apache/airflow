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
import { useEffect } from "react";

import { useParams, useSearchParams } from "react-router-dom";

import { useTaskInstanceServiceGetTaskInstances } from "openapi/queries";

import { SearchParamsKeys } from "src/constants/searchParams";

/**
 * Turn a loop and pass in the url into the coordinate that names one row.
 *
 * A pass is what a reader can choose -- a number they can see -- but it does not address a row:
 * a mapped expansion lives in its own region, and a clear forks later passes into a successor
 * one. Only the server knows which row a pass became, and the list endpoint already answers
 * that, so the pair is resolved there and written back as a coordinate. Everything downstream
 * keeps reading the url, and the resolved link is the one worth sharing.
 *
 * Returns whether the lookup is still in flight. A looped task instance cannot be asked for
 * without a coordinate -- the request is rejected rather than answered empty -- so the caller
 * must hold its children until this clears.
 */
export const useResolvePassCoordinate = (): boolean => {
  const [searchParams, setSearchParams] = useSearchParams();
  const { dagId, mapIndex, runId, taskId } = useParams();
  const loopId = searchParams.get(SearchParamsKeys.LOOP_ID);
  const iteration = searchParams.get(SearchParamsKeys.ITERATION);
  const parsedMapIndex = mapIndex === undefined ? Number.NaN : Number.parseInt(mapIndex, 10);

  const needsLookup =
    searchParams.get(SearchParamsKeys.REGION_ID) === null &&
    loopId !== null &&
    loopId !== "" &&
    iteration !== null &&
    iteration !== "" &&
    dagId !== undefined &&
    runId !== undefined &&
    taskId !== undefined;

  const { data, isFetching } = useTaskInstanceServiceGetTaskInstances(
    {
      dagId: dagId ?? "",
      dagRunId: runId ?? "",
      iteration: needsLookup ? Number(iteration) : undefined,
      limit: 1,
      loopId: loopId ?? undefined,
      mapIndex: Number.isInteger(parsedMapIndex) ? [parsedMapIndex] : undefined,
      taskId: taskId ?? "",
    },
    undefined,
    { enabled: needsLookup },
  );
  const [row] = data?.task_instances ?? [];
  const regionId = row?.region_id;
  const regionIndex = row?.region_index;

  useEffect(() => {
    if (!needsLookup || regionId === undefined || regionIndex === undefined) {
      return;
    }
    const next = new URLSearchParams(searchParams);

    next.set(SearchParamsKeys.REGION_ID, regionId);
    next.set(SearchParamsKeys.REGION_INDEX, String(regionIndex));
    // Replaced rather than pushed: this is the same place, named properly.
    setSearchParams(next, { replace: true });
  }, [needsLookup, regionId, regionIndex, searchParams, setSearchParams]);

  return needsLookup && (isFetching || regionId === undefined);
};
