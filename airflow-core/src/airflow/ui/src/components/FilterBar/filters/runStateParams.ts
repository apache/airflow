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
import { SearchParamsKeys } from "src/constants/searchParams";

import type { FilterValue } from "../types";

// "latest" matches on the latest run only; numeric values are hours; "any" has no time bound.
export type RunStateLookback = "168" | "24" | "720" | "any" | "latest";

export const TIME_LOOKBACKS: ReadonlyArray<RunStateLookback> = ["24", "168", "720"];
export const RUN_STATE_LOOKBACKS: ReadonlyArray<RunStateLookback> = ["latest", ...TIME_LOOKBACKS, "any"];

export type RunStateValue = {
  lookback: RunStateLookback;
  state: string;
};

export const isRunStateValue = (value: FilterValue): value is RunStateValue =>
  typeof value === "object" && value !== null && !Array.isArray(value) && "state" in value;

// The unified Run state pill is backed by the existing independent URL params:
//   "latest" lookback → last_dag_run_state (match the latest run only)
//   a time lookback   → dag_run_state (match any run) + a within-hours bound
//   "any"             → dag_run_state with no time bound
export const runStateToSearchParams = (value: FilterValue): Record<string, string | undefined> => {
  const selection = isRunStateValue(value) ? value : undefined;
  const isLatest = selection?.lookback === "latest";
  const isTimeBounded = selection !== undefined && TIME_LOOKBACKS.includes(selection.lookback);

  return {
    [SearchParamsKeys.DAG_RUN_STATE]: selection !== undefined && !isLatest ? selection.state : undefined,
    [SearchParamsKeys.DAG_RUN_STATE_WITHIN_HOURS]: isTimeBounded ? selection.lookback : undefined,
    [SearchParamsKeys.LAST_DAG_RUN_STATE]: isLatest ? selection.state : undefined,
  };
};

export const runStateFromSearchParams = (params: URLSearchParams): RunStateValue | undefined => {
  const lastRunState = params.get(SearchParamsKeys.LAST_DAG_RUN_STATE);
  const anyRunState = params.get(SearchParamsKeys.DAG_RUN_STATE);
  const withinHours = params.get(SearchParamsKeys.DAG_RUN_STATE_WITHIN_HOURS);

  // The latest-run param wins when both are present, matching how the list endpoint treats them.
  if (lastRunState !== null && lastRunState !== "") {
    return { lookback: "latest", state: lastRunState };
  }
  if (anyRunState === null || anyRunState === "") {
    return undefined;
  }

  const lookback = (TIME_LOOKBACKS as ReadonlyArray<string>).includes(withinHours ?? "")
    ? (withinHours as RunStateLookback)
    : "any";

  return { lookback, state: anyRunState };
};
