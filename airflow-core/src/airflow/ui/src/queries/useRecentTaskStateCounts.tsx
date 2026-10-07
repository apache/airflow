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
import { useDagServiceGetRecentTaskInstanceStateCountsUi } from "openapi/queries";
import type {
  DAGRecentTaskInstanceStateCountsResponse,
  DAGWithLatestDagRunsResponse,
} from "openapi/requests/types.gen";

import { useShowDagsListRecentTasks } from "src/hooks/useUserSettings";
import { useAutoRefresh } from "src/utils";

export type RecentTasks = {
  readonly entriesByDag: Record<string, DAGRecentTaskInstanceStateCountsResponse | undefined>;
  readonly isLoading: boolean;
  /** Whether the user setting shows the counts; when off they are not even fetched. */
  readonly show: boolean;
};

export const useRecentTaskStateCounts = (
  dags: ReadonlyArray<DAGWithLatestDagRunsResponse> | undefined,
): RecentTasks => {
  const [show] = useShowDagsListRecentTasks();
  const refetchInterval = useAutoRefresh({});
  // The counts cover every running run, which may be older than the runs in latest_dag_runs.
  const hasUnfinishedRun = dags?.some((dag) => !dag.is_paused && dag.has_unfinished_runs) ?? false;

  // latest_dag_runs is newest-first and may hold several runs per Dag (14 in card view).
  // Only the latest is sent; the server swaps in the Dag's running runs when it has any.
  // Sorted for a stable query cache key.
  const dagRunIds = (dags ?? [])
    .map((dag) => dag.latest_dag_runs[0]?.id)
    .filter((id): id is number => id !== undefined)
    .sort((left, right) => left - right);

  const { data, isLoading } = useDagServiceGetRecentTaskInstanceStateCountsUi({ dagRunIds }, undefined, {
    enabled: show && dagRunIds.length > 0,
    placeholderData: (prev) => prev,
    refetchInterval: hasUnfinishedRun ? refetchInterval : false,
  });

  return {
    entriesByDag: Object.fromEntries((data?.dags ?? []).map((entry) => [entry.dag_id, entry])),
    isLoading,
    show,
  };
};
