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
import "@testing-library/jest-dom";
import { render, screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";

import type { BackfillDagRunResponse, DagRunState } from "openapi/requests/types.gen";
import { Wrapper } from "src/utils/Wrapper";

import { BackfillProgressBar } from "./BackfillProgressBar";

const createDagRun = (id: number, dagRunState: DagRunState | null): BackfillDagRunResponse => ({
  backfill_id: 7,
  dag_id: "example_dag",
  dag_run_id: `run_${id}`,
  dag_run_state: dagRunState,
  exception_reason: null,
  id,
  logical_date: `2026-07-${String(id).padStart(2, "0")}T00:00:00Z`,
  partition_key: null,
  sort_ordinal: id,
});

describe("BackfillProgressBar", () => {
  it("renders an empty marker when the backfill has no slots", () => {
    render(<BackfillProgressBar dagRuns={[]} total={0} />, { wrapper: Wrapper });

    expect(screen.getByText("—")).toBeInTheDocument();
  });

  it("counts only terminal Dag runs as completed", () => {
    const dagRuns = [
      createDagRun(1, "success"),
      createDagRun(2, "failed"),
      createDagRun(3, "running"),
      createDagRun(4, "queued"),
      createDagRun(5, null),
    ];

    render(<BackfillProgressBar dagRuns={dagRuns} total={5} />, { wrapper: Wrapper });

    expect(screen.getByText("2/5")).toBeInTheDocument();
    expect(screen.getByRole("progressbar")).toHaveAttribute("max", "5");
    expect(screen.getByRole("progressbar")).toHaveAttribute("value", "2");
  });
});
