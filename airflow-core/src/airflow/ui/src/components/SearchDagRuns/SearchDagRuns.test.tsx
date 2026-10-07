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
import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import { MemoryRouter, Route, Routes, useLocation } from "react-router-dom";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { DagRunService } from "openapi/requests/services.gen";
import type { DAGRunCollectionResponse, DAGRunResponse } from "openapi/requests/types.gen";

import { BaseWrapper } from "src/utils/Wrapper";
import type { DagRunSearchOption } from "src/utils/option";

import { SearchDagRuns } from "./SearchDagRuns";

const { loadedOptions, selectedOption } = vi.hoisted<{
  loadedOptions: { current: Array<DagRunSearchOption> };
  selectedOption: { current: DagRunSearchOption };
}>(() => ({
  loadedOptions: { current: [] },
  selectedOption: { current: { label: "run_2", state: "success", value: "run_2" } },
}));

vi.mock("chakra-react-select", () => ({
  AsyncSelect: ({
    loadOptions,
    onChange,
  }: {
    readonly loadOptions: (input: string, callback: (options: Array<DagRunSearchOption>) => void) => void;
    readonly onChange: (option: DagRunSearchOption) => void;
  }) => (
    <>
      <button
        onClick={() =>
          loadOptions("", (options) => {
            loadedOptions.current = options;
          })
        }
        type="button"
      >
        Load Runs
      </button>
      <button onClick={() => onChange(selectedOption.current)} type="button">
        Select Run
      </button>
    </>
  ),
}));

const onClose = vi.fn();

const LocationDisplay = () => <output data-testid="location">{useLocation().pathname}</output>;

const renderSearch = (initialEntry: string, route: string) =>
  render(
    <BaseWrapper>
      <MemoryRouter initialEntries={[initialEntry]}>
        <Routes>
          <Route element={<SearchDagRuns dagId="my_dag" onClose={onClose} />} path={route} />
        </Routes>
        {/* Outside the route, so it still reports where a switch landed when the destination no
            longer matches the path the search was opened from. */}
        <LocationDisplay />
      </MemoryRouter>
    </BaseWrapper>,
  );

const dagRun: DAGRunResponse = {
  bundle_version: null,
  conf: null,
  dag_display_name: "my_dag",
  dag_id: "my_dag",
  dag_run_id: "run_2",
  dag_versions: [],
  data_interval_end: null,
  data_interval_start: null,
  duration: null,
  end_date: null,
  last_scheduling_decision: null,
  logical_date: null,
  note: null,
  partition_date: null,
  partition_key: null,
  queued_at: null,
  run_after: "2025-01-15T00:00:00Z",
  run_type: "manual",
  start_date: null,
  state: "failed",
  team_name: null,
  triggered_by: null,
  triggering_user_name: null,
};

describe("SearchDagRuns", () => {
  beforeEach(() => {
    loadedOptions.current = [];
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  it("offers each run by its run id and its state", async () => {
    const response: DAGRunCollectionResponse = { dag_runs: [dagRun], total_entries: 1 };

    vi.spyOn(DagRunService, "getDagRuns").mockResolvedValue(response);
    renderSearch("/dags/my_dag/runs/run_1", "/dags/:dagId/runs/:runId");

    fireEvent.click(screen.getByRole("button", { name: "Load Runs" }));

    await waitFor(() =>
      expect(loadedOptions.current).toEqual([{ label: "run_2", state: "failed", value: "run_2" }]),
    );
  });

  it("searches only the runs of the Dag it was given, newest first", async () => {
    const getDagRuns = vi
      .spyOn(DagRunService, "getDagRuns")
      .mockResolvedValue({ dag_runs: [], total_entries: 0 });

    renderSearch("/dags/my_dag/runs/run_1", "/dags/:dagId/runs/:runId");

    fireEvent.click(screen.getByRole("button", { name: "Load Runs" }));

    await waitFor(() =>
      expect(getDagRuns).toHaveBeenCalledWith(
        expect.objectContaining({ dagId: "my_dag", orderBy: ["-run_after"], runIdPattern: "" }),
      ),
    );
  });

  it("switches to the selected run and closes the panel", () => {
    renderSearch("/dags/my_dag/runs/run_1", "/dags/:dagId/runs/:runId");

    fireEvent.click(screen.getByRole("button", { name: "Select Run" }));

    expect(screen.getByTestId("location").textContent).toBe("/dags/my_dag/runs/run_2");
    expect(onClose).toHaveBeenCalled();
  });

  it("keeps the task in view when switching runs from a task instance", () => {
    renderSearch("/dags/my_dag/runs/run_1/tasks/task_1", "/dags/:dagId/runs/:runId/tasks/:taskId");

    fireEvent.click(screen.getByRole("button", { name: "Select Run" }));

    expect(screen.getByTestId("location").textContent).toBe("/dags/my_dag/runs/run_2/tasks/task_1");
  });

  it("drops the map index, which is not guaranteed to exist in the run picked", () => {
    renderSearch(
      "/dags/my_dag/runs/run_1/tasks/task_1/mapped/3",
      "/dags/:dagId/runs/:runId/tasks/:taskId/mapped/:mapIndex",
    );

    fireEvent.click(screen.getByRole("button", { name: "Select Run" }));

    expect(screen.getByTestId("location").textContent).toBe("/dags/my_dag/runs/run_2/tasks/task_1");
  });

  it("keeps the task group in view when switching runs from one", () => {
    renderSearch(
      "/dags/my_dag/runs/run_1/tasks/group/group_1",
      "/dags/:dagId/runs/:runId/tasks/group/:groupId",
    );

    fireEvent.click(screen.getByRole("button", { name: "Select Run" }));

    expect(screen.getByTestId("location").textContent).toBe("/dags/my_dag/runs/run_2/tasks/group/group_1");
  });

  it("picks a run from a page that has none selected", () => {
    renderSearch("/dags/my_dag/tasks/task_1", "/dags/:dagId/tasks/:taskId");

    fireEvent.click(screen.getByRole("button", { name: "Select Run" }));

    expect(screen.getByTestId("location").textContent).toBe("/dags/my_dag/runs/run_2/tasks/task_1");
  });
});
