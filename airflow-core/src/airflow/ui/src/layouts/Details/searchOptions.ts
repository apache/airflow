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
import type { DAGRunResponse, DAGWithLatestDagRunsResponse } from "openapi/requests/types.gen";

import type { DagRunSearchOption, DagSearchOption } from "src/utils/option";

/** How many Dags or runs a breadcrumb search offers before anything is typed, and finds per search. */
export const SEARCH_LIMIT = 10;

export const NEWEST_FIRST = ["-run_after"];

export const buildDagOption = (dag: DAGWithLatestDagRunsResponse): DagSearchOption => ({
  isBackfillable: dag.is_backfillable,
  label: dag.dag_display_name || dag.dag_id,
  state: dag.latest_dag_runs[0]?.state ?? null,
  value: dag.dag_id,
});

export const buildDagRunOption = (dagRun: DAGRunResponse): DagRunSearchOption => ({
  label: dagRun.dag_run_id,
  state: dagRun.state,
  value: dagRun.dag_run_id,
});
