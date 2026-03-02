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
import type { DagRunState, DAGWithLatestDagRunsResponse } from "openapi/requests/types.gen";
import { isStatePending, useAutoRefresh } from "src/utils";

export type DagWithLatest = {
  last_run_start_date: string;
} & DAGWithLatestDagRunsResponse;

export const useDags = ({
  dagDisplayNamePattern,
  dagIdPattern,
  dagRunsLimit,
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
}: {
  dagDisplayNamePattern?: string;
  dagIdPattern?: string;
  dagRunsLimit: number;
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
}) => {
  const refetchInterval = useAutoRefresh({});

  const { data, error, isFetching, isLoading } = useDagServiceGetDagsUi(
    {
      dagDisplayNamePattern,
      dagIdPattern,
      dagRunsLimit,
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
    },
    undefined,
    {
      refetchInterval: (query) =>
        query.state.data?.dags.some(
          (dag) => !dag.is_paused && dag.latest_dag_runs.some((dr) => isStatePending(dr.state)),
        )
          ? refetchInterval
          : false,
    },
  );

  return {
    data,
    error,
    isFetching,
    isLoading,
  };
};
