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
import { useEffect, useRef } from "react";

import { useSearchParams } from "react-router-dom";

import { useDagRunServiceGetDagRun, useGridServiceGetLoopSummary } from "openapi/queries";

import { SearchParamsKeys } from "src/constants/searchParams";
import { isStatePending, useAutoRefresh } from "src/utils";

import { useIsLoopGroup } from "./useIsLoopGroup";

/** Runtime summary of a looped Task Group; polls while the Dag run is pending. */
export const useLoopSummary = ({
  dagId,
  groupId,
  runId,
}: {
  dagId: string;
  groupId: string;
  runId: string;
}) => {
  const refetchInterval = useAutoRefresh({ dagId });
  const [searchParams] = useSearchParams();
  const isLoopGroup = useIsLoopGroup(groupId);
  const { data: dagRun } = useDagRunServiceGetDagRun({ dagId, dagRunId: runId }, undefined, {
    enabled: Boolean(dagId) && Boolean(runId),
    refetchInterval: (query) => isStatePending(query.state.data?.state) && refetchInterval,
  });
  const isRunPending = isStatePending(dagRun?.state);
  const enabled = Boolean(dagId) && Boolean(groupId) && Boolean(runId) && isLoopGroup;

  const summaryQuery = useGridServiceGetLoopSummary(
    {
      dagId,
      groupId,
      loopRegionId: searchParams.get(SearchParamsKeys.REGION_ID) ?? undefined,
      runId,
    },
    undefined,
    {
      enabled,
      refetchInterval: isRunPending && refetchInterval,
      retry: false,
    },
  );

  // Polling stops as soon as the run is terminal; fetch once more so the final iteration states land.
  const wasPendingRef = useRef(false);
  const { refetch } = summaryQuery;

  useEffect(() => {
    if (wasPendingRef.current && !isRunPending && enabled) {
      void refetch();
    }
    wasPendingRef.current = isRunPending;
  }, [enabled, isRunPending, refetch]);

  return summaryQuery;
};
