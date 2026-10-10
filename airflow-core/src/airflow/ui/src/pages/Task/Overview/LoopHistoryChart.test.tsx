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
import "@testing-library/jest-dom/vitest";
import { fireEvent, render, screen } from "@testing-library/react";
import { MemoryRouter, useLocation } from "react-router-dom";
import { describe, expect, it, vi } from "vitest";

import { useLoopHistory } from "src/queries/useLoopHistory";
import { BaseWrapper } from "src/utils/Wrapper";

import { LoopHistoryChart } from "./LoopHistoryChart";

vi.mock("src/queries/useLoopHistory", () => ({ useLoopHistory: vi.fn() }));
vi.mock("react-chartjs-2", () => ({
  Bar: ({
    options,
  }: {
    readonly options: { onClick: (event: unknown, elements: Array<{ index: number }>) => void };
  }) => (
    <button
      data-testid="history-chart"
      onClick={() => options.onClick(undefined, [{ index: 0 }])}
      type="button"
    >
      bar
    </button>
  ),
}));

const Location = () => <output data-testid="location">{useLocation().search}</output>;

describe("LoopHistoryChart", () => {
  it("counts only runs that ran to the cap as cap hits", () => {
    vi.mocked(useLoopHistory).mockReturnValue({
      data: {
        dag_id: "dag",
        group_id: "loop",
        runs: [
          {
            iterations_ran: 3,
            max_iterations: 3,
            reason: null,
            run_after: "2026-09-28T00:00:00Z",
            run_id: "capped",
            status: "ran_to_cap",
          },
          {
            iterations_ran: 1,
            max_iterations: 3,
            reason: null,
            run_after: "2026-09-29T00:00:00Z",
            run_id: "early",
            status: "stopped_early",
          },
          {
            iterations_ran: 3,
            max_iterations: 3,
            reason: "iteration_failed",
            run_after: "2026-09-30T00:00:00Z",
            run_id: "failed-on-last",
            status: "failed",
          },
          {
            iterations_ran: 3,
            max_iterations: 3,
            reason: null,
            run_after: "2026-10-01T00:00:00Z",
            run_id: "still-running",
            status: "running",
          },
        ],
      },
    } as ReturnType<typeof useLoopHistory>);
    render(
      <BaseWrapper>
        <MemoryRouter>
          <LoopHistoryChart groupId="loop" />
        </MemoryRouter>
      </BaseWrapper>,
    );
    expect(screen.getByText("loop.history.stats.capHits").parentElement).toHaveTextContent(/capHits1$/u);
  });

  it("opens the selected nested invocation from a history bar", () => {
    vi.mocked(useLoopHistory).mockReturnValue({
      data: {
        dag_id: "dag",
        group_id: "loop",
        runs: [
          {
            iterations_ran: 2,
            loop_region_id: "selected-nested-region",
            max_iterations: 2,
            reason: "cap_reached",
            run_after: "2026-09-28T00:00:00Z",
            run_id: "run",
            status: "ran_to_cap",
          },
        ],
      },
    } as ReturnType<typeof useLoopHistory>);
    render(
      <BaseWrapper>
        <MemoryRouter>
          <LoopHistoryChart groupId="loop" />
          <Location />
        </MemoryRouter>
      </BaseWrapper>,
    );
    fireEvent.click(screen.getByRole("button", { name: "bar" }));
    expect(screen.getByTestId("location")).toHaveTextContent("loop_region_id=selected-nested-region");
  });
});
