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
import { describe, expect, it } from "vitest";

import type { DAGRunResponse, DAGWithLatestDagRunsResponse } from "openapi/requests/types.gen";

import { buildDagOption, buildDagRunOption } from "./searchOptions";

const dagRun = { dag_run_id: "run_1", state: "success" } as DAGRunResponse;
const dag = {
  dag_display_name: "My Dag",
  dag_id: "my_dag",
  latest_dag_runs: [],
} as unknown as DAGWithLatestDagRunsResponse;

describe("searchOptions", () => {
  // react-select keeps keyboard focus only while the focused option is still in `options` by
  // reference. TanStack hands back the same row object for an unchanged row, so an option built
  // from it has to come back unchanged too, or arrowing through the list resets on every poll.
  it("gives a run row the same option object every time", () => {
    expect(buildDagRunOption(dagRun)).toBe(buildDagRunOption(dagRun));
  });

  it("gives a Dag row the same option object every time", () => {
    expect(buildDagOption(dag)).toBe(buildDagOption(dag));
  });

  it("builds a fresh option for a row that changed", () => {
    const changed = { ...dagRun, state: "failed" } as DAGRunResponse;

    expect(buildDagRunOption(changed)).not.toBe(buildDagRunOption(dagRun));
    expect(buildDagRunOption(changed).state).toBe("failed");
  });
});
