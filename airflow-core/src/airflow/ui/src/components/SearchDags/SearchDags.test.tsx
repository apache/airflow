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
import { MemoryRouter, useLocation, useMatches, useNavigate } from "react-router-dom";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { DagService } from "openapi/requests/services.gen";
import type { DAGWithLatestDagRunsCollectionResponse } from "openapi/requests/types.gen";
import { TabEntity, TabName } from "src/constants/tab";
import { BaseWrapper } from "src/utils/Wrapper";
import type { DagSearchOption } from "src/utils/option";

import { SearchDags } from "./SearchDags";

vi.mock("react-router-dom", async (importOriginal) => {
  const original = await importOriginal();

  return { ...(original as object), useMatches: vi.fn() };
});

const { loadedOption, selectedOption } = vi.hoisted<{
  loadedOption: { current: DagSearchOption | undefined };
  selectedOption: { current: DagSearchOption };
}>(() => ({
  loadedOption: { current: undefined },
  selectedOption: {
    current: {
      isBackfillable: true,
      label: "New Dag",
      state: null,
      value: "new_dag",
    },
  },
}));

vi.mock("chakra-react-select", () => ({
  AsyncSelect: ({
    loadOptions,
    onChange,
  }: {
    readonly loadOptions: (input: string, callback: (options: Array<DagSearchOption>) => void) => void;
    readonly onChange: (option: DagSearchOption) => void;
  }) => (
    <>
      <button
        onClick={() =>
          loadOptions("", (options) => {
            [loadedOption.current] = options;
          })
        }
        type="button"
      >
        Load Dags
      </button>
      <button onClick={() => onChange(loadedOption.current ?? selectedOption.current)} type="button">
        Select Dag
      </button>
    </>
  ),
}));

const LocationDisplay = () => {
  const { pathname } = useLocation();
  const navigate = useNavigate();

  return (
    <>
      <output data-testid="location">{pathname}</output>
      <button onClick={() => void navigate(-1)} type="button">
        Back
      </button>
      <button onClick={() => void navigate(1)} type="button">
        Forward
      </button>
    </>
  );
};

const renderSearch = ({
  initialEntry,
  onClose = vi.fn(),
}: {
  initialEntry: string;
  onClose?: () => void;
}) => {
  render(
    <BaseWrapper>
      <MemoryRouter initialEntries={[initialEntry]}>
        <SearchDags onClose={onClose} />
        <LocationDisplay />
      </MemoryRouter>
    </BaseWrapper>,
  );

  return onClose;
};

describe("SearchDags", () => {
  beforeEach(() => {
    loadedOption.current = undefined;
    selectedOption.current = { ...selectedOption.current, isBackfillable: true };
    vi.mocked(useMatches).mockReturnValue([
      {
        data: undefined,
        handle: { entity: TabEntity.Dag, tab: TabName.Details },
        id: "dag-details",
        loaderData: undefined,
        params: { dagId: "old_dag" },
        pathname: "/dags/old_dag/details",
      },
    ]);
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  it("preserves the selected Dag tab when switching Dags", () => {
    const onClose = renderSearch({ initialEntry: "/dags/old_dag/details" });

    fireEvent.click(screen.getByRole("button", { name: "Select Dag" }));

    expect(screen.getByTestId("location").textContent).toBe("/dags/new_dag/details");
    expect(onClose).toHaveBeenCalled();
  });

  it("resets to the Dag overview from a deeper entity route", () => {
    vi.mocked(useMatches).mockReturnValue([
      {
        data: undefined,
        handle: undefined,
        id: "task",
        loaderData: undefined,
        params: { dagId: "old_dag", runId: "run_1", taskId: "task_1" },
        pathname: "/dags/old_dag/runs/run_1/tasks/task_1/details",
      },
    ]);
    renderSearch({ initialEntry: "/dags/old_dag/runs/run_1/tasks/task_1/details" });

    fireEvent.click(screen.getByRole("button", { name: "Select Dag" }));

    expect(screen.getByTestId("location").textContent).toBe("/dags/new_dag");
  });

  it("preserves the backfills tab for a backfillable Dag", () => {
    vi.mocked(useMatches).mockReturnValue([
      {
        data: undefined,
        handle: { entity: TabEntity.Dag, tab: TabName.Backfills },
        id: "dag-backfills",
        loaderData: undefined,
        params: { dagId: "old_dag" },
        pathname: "/dags/old_dag/backfills",
      },
    ]);
    renderSearch({ initialEntry: "/dags/old_dag/backfills" });

    fireEvent.click(screen.getByRole("button", { name: "Select Dag" }));

    expect(screen.getByTestId("location").textContent).toBe("/dags/new_dag/backfills");
  });

  it("maps API backfill support into the option used to preserve the backfills tab", async () => {
    const response: DAGWithLatestDagRunsCollectionResponse = {
      dags: [
        {
          allowed_run_types: null,
          asset_expression: null,
          bundle_name: null,
          bundle_version: null,
          dag_display_name: "New Dag",
          dag_id: "new_dag",
          description: null,
          file_token: "",
          fileloc: "/dags/new_dag.py",
          has_import_errors: false,
          has_task_concurrency_limits: false,
          is_backfillable: false,
          is_favorite: false,
          is_paused: false,
          is_stale: false,
          last_expired: null,
          last_parse_duration: null,
          last_parsed_time: null,
          latest_dag_runs: [],
          max_active_runs: 16,
          max_active_tasks: 16,
          max_consecutive_failed_dag_runs: 0,
          next_dagrun_data_interval_end: null,
          next_dagrun_data_interval_start: null,
          next_dagrun_logical_date: null,
          next_dagrun_run_after: null,
          owners: ["airflow"],
          pending_actions: [],
          relative_fileloc: "new_dag.py",
          tags: [],
          timetable_description: null,
          timetable_partitioned: false,
          timetable_periodic: false,
          timetable_summary: null,
        },
      ],
      total_entries: 1,
    };

    vi.spyOn(DagService, "getDagsUi").mockResolvedValue(response);
    vi.mocked(useMatches).mockReturnValue([
      {
        data: undefined,
        handle: { entity: TabEntity.Dag, tab: TabName.Backfills },
        id: "dag-backfills",
        loaderData: undefined,
        params: { dagId: "old_dag" },
        pathname: "/dags/old_dag/backfills",
      },
    ]);
    renderSearch({ initialEntry: "/dags/old_dag/backfills" });

    fireEvent.click(screen.getByRole("button", { name: "Load Dags" }));
    await waitFor(() => expect(loadedOption.current?.isBackfillable).toBe(false));
    fireEvent.click(screen.getByRole("button", { name: "Select Dag" }));

    expect(screen.getByTestId("location").textContent).toBe("/dags/new_dag");
  });

  it("resets plugin routes when destination compatibility is unknown", () => {
    vi.mocked(useMatches).mockReturnValue([
      {
        data: undefined,
        handle: undefined,
        id: "dag-plugin",
        loaderData: undefined,
        params: { "*": "nested/detail/42", dagId: "old_dag", page: "test" },
        pathname: "/dags/old_dag/plugin/test/nested/detail/42",
      },
    ]);
    renderSearch({ initialEntry: "/dags/old_dag/plugin/test/nested/detail/42" });

    fireEvent.click(screen.getByRole("button", { name: "Select Dag" }));

    expect(screen.getByTestId("location").textContent).toBe("/dags/new_dag");
  });

  it("keeps browser back and forward history after switching Dags", () => {
    renderSearch({ initialEntry: "/dags/old_dag/details" });

    fireEvent.click(screen.getByRole("button", { name: "Select Dag" }));
    fireEvent.click(screen.getByRole("button", { name: "Back" }));
    expect(screen.getByTestId("location").textContent).toBe("/dags/old_dag/details");

    fireEvent.click(screen.getByRole("button", { name: "Forward" }));
    expect(screen.getByTestId("location").textContent).toBe("/dags/new_dag/details");
  });
});
