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
import type { RunStateLookback } from "./RunStateLookbackSelect";

// The unified "Run state" control is backed by the existing independent URL params:
//   - "latest" lookback  → last_dag_run_state  (match the latest run only)
//   - a time lookback     → dag_run_state       (match any run) + a within-hours bound
//     ("any" is any-run with no time bound, so it carries no lookback param)
// An unset key means "clear this param".
export type RunStateSelection = {
  readonly dagRunState?: string;
  readonly dagRunStateWithinHours?: string;
  readonly lastDagRunState?: string;
};

export const runStateSelectionFor = (
  state: string | undefined,
  lookback: RunStateLookback,
): RunStateSelection => {
  if (state === undefined) {
    return {};
  }
  if (lookback === "latest") {
    return { lastDagRunState: state };
  }
  if (lookback === "any") {
    return { dagRunState: state };
  }

  return { dagRunState: state, dagRunStateWithinHours: lookback };
};

export const runLookbackFor = (
  lastRunState: string | null,
  anyRunState: string | null,
  anyRunStateLookback: string | null,
): RunStateLookback =>
  lastRunState !== null || anyRunState === null
    ? "latest"
    : ((anyRunStateLookback ?? "any") as RunStateLookback);
