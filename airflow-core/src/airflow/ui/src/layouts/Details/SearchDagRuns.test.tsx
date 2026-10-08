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
import type { DAGRunResponse } from "openapi/requests/types.gen";

import { BaseWrapper } from "src/utils/Wrapper";
import type { DagRunSearchOption } from "src/utils/option";

import { SearchDagRuns } from "./SearchDagRuns";

const { searched } = vi.hoisted<{ searched: { current: Array<DagRunSearchOption> } }>(() => ({
  searched: { current: [] },
}));

// Stands in for the real combo box: it lists whatever the panel was handed without a search, and
// offers one button that runs a search, which is the split this component is built around.
vi.mock("chakra-react-select", () => ({
  AsyncSelect: ({
    defaultOptions,
    loadOptions,
    onChange,
  }: {
    readonly defaultOptions: Array<DagRunSearchOption> | true;
    readonly loadOptions: (input: string, callback: (options: Array<DagRunSearchOption>) => void) => void;
    readonly onChange: (option: DagRunSearchOption) => void;
  }) => (
    <>
      {(Array.isArray(defaultOptions) ? defaultOptions : []).map((option) => (
        <button key={option.value} onClick={() => onChange(option)} type="button">
          {`${option.label} (${option.state})`}
        </button>
      ))}
      <button
        onClick={() =>
          loadOptions("older", (options) => {
            searched.current = options;
          })
        }
        type="button"
      >
        Search Runs
      </button>
    </>
  ),
}));

const onClose = vi.fn();

const LocationDisplay = () => <output data-testid="location">{useLocation().pathname}</output>;

const renderSearch = (initialEntry: string, route: string, isMapped = false) =>
  render(
    <BaseWrapper>
      <MemoryRouter initialEntries={[initialEntry]}>
        <Routes>
          <Route
            element={
              <SearchDagRuns
                dagId="my_dag"
                isMapped={isMapped}
                onClose={onClose}
                runs={[{ label: "run_2", state: "failed", value: "run_2" }]}
              />
            }
            path={route}
          />
        </Routes>
        {/* Outside the route, so it still reports where a switch landed when the destination no
            longer matches the path the search was opened from. */}
        <LocationDisplay />
      </MemoryRouter>
    </BaseWrapper>,
  );

const buildRun = (dagRunId: string): DAGRunResponse => ({
  bundle_version: null,
  conf: null,
  dag_display_name: "my_dag",
  dag_id: "my_dag",
  dag_run_id: dagRunId,
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
});

/** The listed run, as the mocked combo box renders it. */
const RUN_2 = "run_2 (failed)";

describe("SearchDagRuns", () => {
  beforeEach(() => {
    searched.current = [];
    vi.spyOn(DagRunService, "getDagRuns").mockResolvedValue({
      dag_runs: [buildRun("run_2")],
      total_entries: 1,
    });
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  it("lists the runs it was handed, by run id and state, without asking for any", () => {
    renderSearch("/dags/my_dag/runs/run_1", "/dags/:dagId/runs/:runId");

    expect(screen.getByRole("button", { name: RUN_2 })).toBeInTheDocument();
    // Opening the panel is not a reason to go to the server: the level above it already has these.
    expect(DagRunService.getDagRuns).not.toHaveBeenCalled();
  });

  it("reaches past the listed runs when searched", async () => {
    renderSearch("/dags/my_dag/runs/run_1", "/dags/:dagId/runs/:runId");

    fireEvent.click(screen.getByRole("button", { name: "Search Runs" }));

    await waitFor(() =>
      expect(DagRunService.getDagRuns).toHaveBeenCalledWith(
        expect.objectContaining({ dagId: "my_dag", runIdPattern: "older" }),
      ),
    );
    expect(searched.current).toEqual([{ label: "run_2", state: "failed", value: "run_2" }]);
  });

  it("switches to the selected run and closes the panel", () => {
    renderSearch("/dags/my_dag/runs/run_1", "/dags/:dagId/runs/:runId");

    fireEvent.click(screen.getByRole("button", { name: RUN_2 }));

    expect(screen.getByTestId("location").textContent).toBe("/dags/my_dag/runs/run_2");
    expect(onClose).toHaveBeenCalled();
  });

  it("keeps the task in view when switching runs from a task instance", () => {
    renderSearch("/dags/my_dag/runs/run_1/tasks/task_1", "/dags/:dagId/runs/:runId/tasks/:taskId");

    fireEvent.click(screen.getByRole("button", { name: RUN_2 }));

    expect(screen.getByTestId("location").textContent).toBe("/dags/my_dag/runs/run_2/tasks/task_1");
  });

  it("lands an expanded task on its list of instances, not on a map index", () => {
    renderSearch(
      "/dags/my_dag/runs/run_1/tasks/task_1/mapped/3",
      "/dags/:dagId/runs/:runId/tasks/:taskId/mapped/:mapIndex",
      true,
    );

    fireEvent.click(screen.getByRole("button", { name: RUN_2 }));

    // `/tasks/task_1` would be a row the run picked is not guaranteed to have, and renders
    // "No task instance found".
    expect(screen.getByTestId("location").textContent).toBe("/dags/my_dag/runs/run_2/tasks/task_1/mapped");
  });

  it("keeps the open task instance tab across the switch", () => {
    renderSearch("/dags/my_dag/runs/run_1/tasks/task_1/xcom", "/dags/:dagId/runs/:runId/tasks/:taskId/xcom");

    fireEvent.click(screen.getByRole("button", { name: RUN_2 }));

    expect(screen.getByTestId("location").textContent).toBe("/dags/my_dag/runs/run_2/tasks/task_1/xcom");
  });

  it("keeps the task group in view when switching runs from one", () => {
    renderSearch(
      "/dags/my_dag/runs/run_1/tasks/group/group_1",
      "/dags/:dagId/runs/:runId/tasks/group/:groupId",
    );

    fireEvent.click(screen.getByRole("button", { name: RUN_2 }));

    expect(screen.getByTestId("location").textContent).toBe("/dags/my_dag/runs/run_2/tasks/group/group_1");
  });

  it("picks a run from a page that has none selected", () => {
    renderSearch("/dags/my_dag/tasks/task_1", "/dags/:dagId/tasks/:taskId");

    fireEvent.click(screen.getByRole("button", { name: RUN_2 }));

    expect(screen.getByTestId("location").textContent).toBe("/dags/my_dag/runs/run_2/tasks/task_1");
  });
});
