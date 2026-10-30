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
import type { PropsWithChildren } from "react";

import "@testing-library/jest-dom/vitest";
import { render, screen } from "@testing-library/react";
import { MemoryRouter, Route, Routes } from "react-router-dom";
import { beforeEach, describe, expect, it, vi } from "vitest";

import type { LoopSummaryResponse } from "openapi/requests/types.gen";

import { TimezoneProvider } from "src/context/timezone";
import { BaseWrapper } from "src/utils/Wrapper";

import { LoopIterations } from "./LoopIterations";

let mockSummary: LoopSummaryResponse | undefined;

vi.mock("src/queries/useLoopSummary", () => ({
  useLoopSummary: () => ({ data: mockSummary }),
}));

const GROUP_PATH = "/dags/my_dag/runs/run_1/tasks/group/refine";

const buildSummary = (overrides: Partial<LoopSummaryResponse> = {}): LoopSummaryResponse => ({
  dag_id: "my_dag",
  group_id: "refine",
  iterations: [],
  iterations_ran: 2,
  max_iterations: 3,
  run_id: "run_1",
  status: "stopped_early",
  ...overrides,
});

const renderAt = (search = "") => {
  const wrapper = ({ children }: PropsWithChildren) => (
    <BaseWrapper>
      <MemoryRouter initialEntries={[`${GROUP_PATH}/iterations${search}`]}>
        <TimezoneProvider>
          <Routes>
            {/* Parameterised so useParams gives the component its dag, run and group. */}
            <Route element={children} path="/dags/:dagId/runs/:runId/tasks/group/:groupId/iterations" />
          </Routes>
        </TimezoneProvider>
      </MemoryRouter>
    </BaseWrapper>
  );

  return render(<LoopIterations />, { wrapper });
};

beforeEach(() => {
  mockSummary = undefined;
});

describe("LoopIterations", () => {
  it("renders a row per pass with its decision reason", () => {
    mockSummary = buildSummary({
      iterations: [
        { decision_reason: "error 0.17, needs < 1e-06", index: 1, state: "success", task_count: 2 },
        { decision_reason: "error 8.9e-07, needs < 1e-06", index: 2, state: "success", task_count: 2 },
      ],
      stopped_at_iteration: 2,
    });

    renderAt();

    expect(screen.getByText("error 0.17, needs < 1e-06")).toBeInTheDocument();
    expect(screen.getByText("error 8.9e-07, needs < 1e-06")).toBeInTheDocument();
  });

  it("links each pass to the group's task instances scoped to it", () => {
    mockSummary = buildSummary({
      iterations: [
        { index: 1, state: "success" },
        { index: 2, state: "failed" },
      ],
    });

    renderAt();

    // The i18n mock echoes the key, so every row's link carries the same accessible name.
    const links = screen.getAllByRole("link");
    // Newest first, so the top row is the last pass that ran.
    const href = links[0]?.getAttribute("href") ?? "";
    const [pathname, search] = href.split("?");

    expect(links).toHaveLength(2);
    expect(links.map((link) => link.textContent)).toStrictEqual(["2", "1"]);
    const params = new URLSearchParams(search);

    expect(pathname).toBe(GROUP_PATH);
    expect(params.get("loop_id")).toBe("refine");
    expect(params.get("iteration")).toBe("2");
  });

  it("drops a pinned row and any paging when a pass is picked", () => {
    mockSummary = buildSummary({ iterations: [{ index: 1, state: "success" }] });

    renderAt("?region_id=r1&region_index=0&offset=20&dag_run_state=failed");

    const href = screen.getAllByRole("link")[0]?.getAttribute("href") ?? "";
    const params = new URLSearchParams(href.split("?")[1]);

    expect(params.get("region_id")).toBeNull();
    expect(params.get("region_index")).toBeNull();
    expect(params.get("offset")).toBeNull();
    // Filters the page was reached with are not this control's to discard.
    expect(params.get("dag_run_state")).toBe("failed");
  });

  it("says so when the loop has reported no passes yet", () => {
    mockSummary = buildSummary({ iterations: [] });

    renderAt();

    expect(screen.getByText(/noItemsFound/iu)).toBeInTheDocument();
  });
});
