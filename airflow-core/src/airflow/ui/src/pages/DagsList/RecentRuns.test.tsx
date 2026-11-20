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
import { render, screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";

import type { DAGWithLatestDagRunsResponse } from "openapi/requests/types.gen";

import { Wrapper } from "src/utils/Wrapper";

import { RecentRuns } from "./RecentRuns";

type LatestRun = DAGWithLatestDagRunsResponse["latest_dag_runs"][number];

const run = (duration: number | null, runId: string): LatestRun =>
  ({
    dag_id: "dag",
    duration,
    end_date: duration === null ? null : "2026-10-21T00:00:10Z",
    run_id: runId,
    start_date: "2026-10-21T00:00:00Z",
    state: duration === null ? "queued" : "success",
  }) as LatestRun;

const barRow = () => screen.getAllByTestId("recent-run")[0]?.parentElement;

describe("RecentRuns", () => {
  it("holds the full bar height so pending runs stay on the baseline", () => {
    render(<RecentRuns latestRuns={[run(null, "pending-1"), run(null, "pending-2")]} />, {
      wrapper: Wrapper,
    });

    // Without a height of its own the row collapses to the stub height, and the card's reserved
    // space pushes the whole group to the top instead of the baseline.
    expect(barRow()).toHaveStyle({ minHeight: "65px" });
  });

  it("keeps the same floor once a run has a duration", () => {
    render(<RecentRuns latestRuns={[run(10, "done-1")]} />, { wrapper: Wrapper });

    expect(barRow()).toHaveStyle({ minHeight: "65px" });
  });
});
