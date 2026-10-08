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
import type { DAGWithLatestDagRunsResponse } from "openapi/requests/types.gen";

import type { DagSearchOption } from "src/utils/option";

export const DAG_SEARCH_LIMIT = 10;

const NO_OPTIONS: Array<DagSearchOption> = [];

export const buildDagOption = (dag: DAGWithLatestDagRunsResponse): DagSearchOption => ({
  isBackfillable: dag.is_backfillable,
  label: dag.dag_display_name || dag.dag_id,
  state: dag.latest_dag_runs[0]?.state ?? null,
  value: dag.dag_id,
});

/**
 * The Dags offered before anything is typed.
 *
 * It belongs to the breadcrumb level rather than to the panel the level opens: a query that only
 * starts when the panel is opened has nothing to show until it answers, which is the spinner this
 * avoids.
 */
export const useDagSearchOptions = () => {
  const { data, isLoading } = useDagServiceGetDagsUi({ dagRunsLimit: 1, limit: DAG_SEARCH_LIMIT });

  return { dags: data === undefined ? NO_OPTIONS : data.dags.map(buildDagOption), isLoading };
};
