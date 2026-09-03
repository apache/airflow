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
import { useDagServiceGetLatestRunTaskInstanceStateCountsUi } from "openapi/queries";
import type { DAGWithLatestDagRunsResponse } from "openapi/requests/types.gen";

import { isStatePending, useAutoRefresh } from "src/utils";

export const useLatestRunTaskStateCounts = ({
  dags,
}: {
  readonly dags: ReadonlyArray<DAGWithLatestDagRunsResponse> | undefined;
}) => {
  const refetchInterval = useAutoRefresh({});
  const hasPendingRun =
    dags?.some((dag) => !dag.is_paused && dag.latest_dag_runs.some((run) => isStatePending(run.state))) ??
    false;

  // latest_dag_runs is newest-first and may hold several runs per Dag (14 in card view),
  // but only the latest is counted. Sorted for a stable query cache key.
  const dagRunIds = (dags ?? [])
    .map((dag) => dag.latest_dag_runs[0]?.id)
    .filter((id): id is number => id !== undefined)
    .sort((left, right) => left - right);

  return useDagServiceGetLatestRunTaskInstanceStateCountsUi({ dagRunIds }, undefined, {
    enabled: dagRunIds.length > 0,
    placeholderData: (prev) => prev,
    refetchInterval: hasPendingRun ? refetchInterval : false,
  });
};
