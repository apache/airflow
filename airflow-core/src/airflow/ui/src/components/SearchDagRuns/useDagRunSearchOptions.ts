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
import { useDagRunServiceGetDagRuns } from "openapi/queries";
import type { DAGRunResponse } from "openapi/requests/types.gen";

import { useAutoRefresh } from "src/utils";
import type { DagRunSearchOption } from "src/utils/option";

export const DAG_RUN_SEARCH_LIMIT = 10;
export const NEWEST_FIRST = ["-run_after"];

const NO_OPTIONS: Array<DagRunSearchOption> = [];

export const buildDagRunOption = (dagRun: DAGRunResponse): DagRunSearchOption => ({
  label: dagRun.dag_run_id,
  state: dagRun.state,
  value: dagRun.dag_run_id,
});

/**
 * The Dag's most recent runs, as search options.
 *
 * It belongs to the breadcrumb level rather than to the panel the level opens: a query that only
 * starts when the panel is opened has nothing to show until it answers, which is the spinner this
 * avoids. Living a level up, it is already loaded by the time the panel opens, and the page's
 * auto-refresh keeps it that way.
 */
export const useDagRunSearchOptions = (dagId: string) => {
  // Unlike the grid, this list has to notice runs that do not exist yet, so it keeps polling once
  // they have all finished — `checkPendingRuns` only scales the interval back, it does not stop.
  const refetchInterval = useAutoRefresh({ checkPendingRuns: true, dagId });

  const { data, isLoading } = useDagRunServiceGetDagRuns(
    { dagId, limit: DAG_RUN_SEARCH_LIMIT, orderBy: NEWEST_FIRST },
    undefined,
    { refetchInterval },
  );

  return { isLoading, runs: data === undefined ? NO_OPTIONS : data.dag_runs.map(buildDagRunOption) };
};
