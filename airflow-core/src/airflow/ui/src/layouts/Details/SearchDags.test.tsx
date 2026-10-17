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
import type { ReactNode } from "react";

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

const NEW_DAG: DagSearchOption = { isBackfillable: true, label: "New Dag", state: null, value: "new_dag" };

// Renders the pieces this component actually drives: the control (which carries whatever the
// caller put at its end, such as the match-anywhere toggle), the input, and the listed options.
vi.mock("chakra-react-select", () => ({
  chakraComponents: { Control: ({ children }: { readonly children: ReactNode }) => <div>{children}</div> },
  Select: ({
    components,
    inputValue,
    onChange,
    onInputChange,
    options,
  }: {
    readonly components: { Control: (props: { readonly children: ReactNode }) => ReactNode };
    readonly inputValue: string;
    readonly onChange: (option: DagSearchOption) => void;
    readonly onInputChange: (value: string, meta: { action: string }) => void;
    readonly options: Array<DagSearchOption>;
  }) => (
    <>
      <components.Control>{undefined}</components.Control>
      <input
        aria-label="Search"
        onChange={(event) => onInputChange(event.target.value, { action: "input-change" })}
        value={inputValue}
      />
      {options.map((option) => (
        <button key={option.value} onClick={() => onChange(option)} type="button">
          {option.label}
        </button>
      ))}
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
  dags = [NEW_DAG],
  initialEntry,
  onClose = vi.fn(),
}: {
  dags?: Array<DagSearchOption>;
  initialEntry: string;
  onClose?: () => void;
}) => {
  render(
    <BaseWrapper>
      <MemoryRouter initialEntries={[initialEntry]}>
        <SearchDags dags={dags} onClose={onClose} />
        <LocationDisplay />
      </MemoryRouter>
    </BaseWrapper>,
  );

  return onClose;
};

const typeSearch = (value: string) =>
  fireEvent.change(screen.getByLabelText("Search"), { target: { value } });

const selectDag = (name = "New Dag") => fireEvent.click(screen.getByRole("button", { name }));

describe("SearchDags", () => {
  beforeEach(() => {
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

  it("matches the start of the name by default", async () => {
    const getDagsUi = vi.spyOn(DagService, "getDagsUi").mockResolvedValue({ dags: [], total_entries: 0 });

    renderSearch({ initialEntry: "/dags/old_dag" });

    typeSearch("cons");

    await waitFor(() =>
      expect(getDagsUi).toHaveBeenCalledWith(
        expect.objectContaining({ dagDisplayNamePrefixPattern: "cons" }),
      ),
    );
    expect(getDagsUi.mock.calls[0]?.[0]).not.toHaveProperty("dagDisplayNamePattern");
  });

  it("matches anywhere in the name once the option is on", async () => {
    const getDagsUi = vi.spyOn(DagService, "getDagsUi").mockResolvedValue({ dags: [], total_entries: 0 });

    // The toggle mirrors its choice into the URL, which is also how a shared link carries it.
    renderSearch({ initialEntry: "/dags/old_dag?advanced_search=breadcrumb-dags" });

    typeSearch("cons");

    await waitFor(() =>
      expect(getDagsUi).toHaveBeenCalledWith(expect.objectContaining({ dagDisplayNamePattern: "cons" })),
    );
    expect(getDagsUi.mock.calls[0]?.[0]).not.toHaveProperty("dagDisplayNamePrefixPattern");
  });

  it("re-runs the search it is showing when match-anywhere is turned on", async () => {
    const getDagsUi = vi.spyOn(DagService, "getDagsUi").mockResolvedValue({ dags: [], total_entries: 0 });

    renderSearch({ initialEntry: "/dags/old_dag" });

    typeSearch("cons");
    await waitFor(() =>
      expect(getDagsUi).toHaveBeenCalledWith(
        expect.objectContaining({ dagDisplayNamePrefixPattern: "cons" }),
      ),
    );

    fireEvent.click(screen.getByTestId("advanced-search-toggle"));

    // The text did not change, so only the toggle can have asked for this one.
    await waitFor(() =>
      expect(getDagsUi).toHaveBeenCalledWith(expect.objectContaining({ dagDisplayNamePattern: "cons" })),
    );
  });

  it("preserves the selected Dag tab when switching Dags", () => {
    const onClose = renderSearch({ initialEntry: "/dags/old_dag/details" });

    selectDag();

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

    selectDag();

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

    selectDag();

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
          has_unfinished_runs: false,
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
    // Nothing listed up front, so the only option on screen is the one the search returned.
    renderSearch({ dags: [], initialEntry: "/dags/old_dag/backfills" });

    typeSearch("new");
    await screen.findByRole("button", { name: "New Dag" });
    selectDag();

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

    selectDag();

    expect(screen.getByTestId("location").textContent).toBe("/dags/new_dag");
  });

  it("keeps browser back and forward history after switching Dags", () => {
    renderSearch({ initialEntry: "/dags/old_dag/details" });

    selectDag();
    fireEvent.click(screen.getByRole("button", { name: "Back" }));
    expect(screen.getByTestId("location").textContent).toBe("/dags/old_dag/details");

    fireEvent.click(screen.getByRole("button", { name: "Forward" }));
    expect(screen.getByTestId("location").textContent).toBe("/dags/new_dag/details");
  });
});
