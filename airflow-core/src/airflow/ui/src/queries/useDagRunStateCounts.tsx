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
import { useDagServiceGetDagRunStateCountsUi } from "openapi/queries";
import type { DAGWithLatestDagRunsResponse } from "openapi/requests/types.gen";
import { isStatePending, useAutoRefresh } from "src/utils";

export const useDagRunStateCounts = ({
  dagIds,
  dags,
}: {
  readonly dagIds: ReadonlyArray<string>;
  // Refresh predicate is derived from useDags' data so the counts query doesn't
  // need to be loaded before it knows whether to poll — avoids a chicken-and-egg.
  readonly dags: ReadonlyArray<DAGWithLatestDagRunsResponse> | undefined;
}) => {
  const refetchInterval = useAutoRefresh({});
  const hasPendingRun =
    dags?.some((dag) => !dag.is_paused && dag.latest_dag_runs.some((run) => isStatePending(run.state))) ??
    false;

  // Stable key: sort the dag_ids so pagination/sort order changes don't churn the cache.
  const sortedDagIds = [...dagIds].sort();

  return useDagServiceGetDagRunStateCountsUi({ dagIds: sortedDagIds }, undefined, {
    enabled: sortedDagIds.length > 0,
    placeholderData: (prev) => prev,
    refetchInterval: hasPendingRun ? refetchInterval : false,
  });
};
