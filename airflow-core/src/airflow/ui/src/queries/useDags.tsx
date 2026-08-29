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
import { useDagServiceGetDagsUi } from "openapi/queries";
import type { DagRunState } from "openapi/requests/types.gen";
import { isStatePending, useAutoRefresh } from "src/utils";

export const useDags = ({
  advancedSearch = false,
  dagDisplayNamePattern,
  dagIdPattern,
  dagRunsLimit,
  dagRunState,
  excludeStale = true,
  isFavorite,
  lastDagRunState,
  limit,
  offset,
  orderBy,
  owners,
  paused,
  pendingHitl,
  tags,
  tagsMatchMode,
  teams,
  timetableType,
}: {
  advancedSearch?: boolean;
  dagDisplayNamePattern?: string;
  dagIdPattern?: string;
  dagRunsLimit: number;
  dagRunState?: DagRunState;
  excludeStale?: boolean;
  isFavorite?: boolean;
  lastDagRunState?: DagRunState;
  limit?: number;
  offset?: number;
  orderBy?: Array<string>;
  owners?: Array<string>;
  paused?: boolean;
  pendingHitl?: boolean;
  tags?: Array<string>;
  tagsMatchMode?: "all" | "any";
  teams?: Array<string>;
  timetableType?: Array<string>;
}) => {
  const refetchInterval = useAutoRefresh({ checkPendingRuns: true });

  const { data, error, isFetching, isLoading } = useDagServiceGetDagsUi(
    {
      ...(advancedSearch
        ? { dagDisplayNamePattern, dagIdPattern }
        : { dagDisplayNamePrefixPattern: dagDisplayNamePattern, dagIdPrefixPattern: dagIdPattern }),
      dagRunsLimit,
      dagRunState,
      excludeStale,
      hasPendingActions: pendingHitl,
      isFavorite,
      lastDagRunState,
      limit,
      offset,
      orderBy,
      owners,
      paused,
      tags,
      tagsMatchMode,
      teams,
      timetableType,
    },
    undefined,
    {
      // Filter changes swap the query key, which would otherwise drop the list to skeletons
      placeholderData: (prev) => prev,
      refetchInterval: (query) =>
        refetchInterval === false
          ? false
          : query.state.data?.dags.some(
                (dag) => !dag.is_paused && dag.latest_dag_runs.some((dr) => isStatePending(dr.state)),
              )
            ? refetchInterval
            : refetchInterval * 10,
    },
  );

  return {
    data,
    error,
    isFetching,
    isLoading,
  };
};
