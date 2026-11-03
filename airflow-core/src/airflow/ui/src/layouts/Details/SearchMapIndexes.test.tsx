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
import { afterEach, describe, expect, it, vi } from "vitest";

import { TaskInstanceService } from "openapi/requests/services.gen";
import type { TaskInstanceResponse } from "openapi/requests/types.gen";

import { BaseWrapper } from "src/utils/Wrapper";
import type { MapIndexSearchOption } from "src/utils/option";

import { SearchMapIndexes } from "./SearchMapIndexes";

vi.mock("chakra-react-select", () => ({
  Select: ({
    inputValue,
    onChange,
    onInputChange,
    options,
  }: {
    readonly inputValue: string;
    readonly onChange: (option: MapIndexSearchOption) => void;
    readonly onInputChange: (value: string, meta: { action: string }) => void;
    readonly options: Array<MapIndexSearchOption>;
  }) => (
    <>
      <input
        aria-label="Search"
        onChange={(event) => onInputChange(event.target.value, { action: "input-change" })}
        value={inputValue}
      />
      {options.map((option) => (
        <button key={option.value} onClick={() => onChange(option)} type="button">
          {option.state === undefined ? option.label : `${option.label} (${option.state})`}
        </button>
      ))}
    </>
  ),
}));

const REGION_ID = "01a140b7-c277-7330-a771-56be0f2440f6";

const onClose = vi.fn();

const LocationDisplay = () => {
  const { pathname, search } = useLocation();

  return (
    <>
      <output data-testid="location">{pathname}</output>
      <output data-testid="search">{search}</output>
    </>
  );
};

const INSTANCES: Array<MapIndexSearchOption> = [
  { label: "alpha", regionId: REGION_ID, regionIndex: 0, state: "success", value: "0" },
  { label: "2", regionId: REGION_ID, regionIndex: 2, state: "failed", value: "2" },
];

const renderSearch = (initialEntry: string, route: string) =>
  render(
    <BaseWrapper>
      <MemoryRouter initialEntries={[initialEntry]}>
        <Routes>
          <Route
            element={
              <SearchMapIndexes
                currentRegionId={REGION_ID}
                dagId="my_dag"
                instances={INSTANCES}
                onClose={onClose}
                runId="run_1"
                taskId="task_1"
              />
            }
            path={route}
          />
        </Routes>
        <LocationDisplay />
      </MemoryRouter>
    </BaseWrapper>,
  );

const MAPPED_ROUTE = "/dags/:dagId/runs/:runId/tasks/:taskId/mapped/:mapIndex";

describe("SearchMapIndexes", () => {
  afterEach(() => vi.restoreAllMocks());

  it("names each instance by its rendered map index, with its state", () => {
    renderSearch("/dags/my_dag/runs/run_1/tasks/task_1/mapped/2", MAPPED_ROUTE);

    expect(screen.getByRole("button", { name: "alpha (success)" })).toBeInTheDocument();
    // An instance with nothing rendered still has to be nameable, so it falls back to its number.
    expect(screen.getByRole("button", { name: "2 (failed)" })).toBeInTheDocument();
  });

  it("offers the whole expansion as a way back, with no state of its own", () => {
    renderSearch("/dags/my_dag/runs/run_1/tasks/task_1/mapped/2", MAPPED_ROUTE);

    // Labelled without a state, since it stands for every instance rather than one of them.
    expect(screen.getByRole("button", { name: "allMapIndexes" })).toBeInTheDocument();
  });

  it("leaves the pass behind only the instance when all map indexes is picked", () => {
    renderSearch(
      `/dags/my_dag/runs/run_1/tasks/task_1/mapped/2?region_id=${REGION_ID}&region_index=2`,
      MAPPED_ROUTE,
    );

    fireEvent.click(screen.getByRole("button", { name: "allMapIndexes" }));

    expect(screen.getByTestId("location").textContent).toBe("/dags/my_dag/runs/run_1/tasks/task_1/mapped");
    expect(screen.getByTestId("search").textContent).toBe(`?region_id=${REGION_ID}`);
  });

  it("switches to the selected instance and closes the panel", () => {
    renderSearch("/dags/my_dag/runs/run_1/tasks/task_1/mapped/2", MAPPED_ROUTE);

    fireEvent.click(screen.getByRole("button", { name: "alpha (success)" }));

    expect(screen.getByTestId("location").textContent).toBe("/dags/my_dag/runs/run_1/tasks/task_1/mapped/0");
    expect(onClose).toHaveBeenCalled();
  });

  it("keeps the open tab across the switch", () => {
    renderSearch("/dags/my_dag/runs/run_1/tasks/task_1/mapped/2/xcom", `${MAPPED_ROUTE}/xcom`);

    fireEvent.click(screen.getByRole("button", { name: "alpha (success)" }));

    expect(screen.getByTestId("location").textContent).toBe(
      "/dags/my_dag/runs/run_1/tasks/task_1/mapped/0/xcom",
    );
  });

  it("searches the task's instances by rendered map index", async () => {
    const getMapped = vi
      .spyOn(TaskInstanceService, "getMappedTaskInstances")
      .mockResolvedValue({ task_instances: [], total_entries: 0 });

    renderSearch("/dags/my_dag/runs/run_1/tasks/task_1/mapped/2", MAPPED_ROUTE);

    fireEvent.change(screen.getByLabelText("Search"), { target: { value: "alpha" } });

    await waitFor(() =>
      expect(getMapped).toHaveBeenCalledWith(
        expect.objectContaining({
          dagId: "my_dag",
          dagRunId: "run_1",
          renderedMapIndexPattern: "alpha",
          taskId: "task_1",
        }),
      ),
    );
  });

  it("offers what the search found", async () => {
    const instance = {
      map_index: 3,
      rendered_map_index: "alpha",
      state: "running",
    } as TaskInstanceResponse;

    vi.spyOn(TaskInstanceService, "getMappedTaskInstances").mockResolvedValue({
      task_instances: [instance],
      total_entries: 1,
    });

    renderSearch("/dags/my_dag/runs/run_1/tasks/task_1/mapped/2", MAPPED_ROUTE);

    fireEvent.change(screen.getByLabelText("Search"), { target: { value: "alpha" } });

    expect(await screen.findByRole("button", { name: "alpha (running)" })).toBeInTheDocument();
  });
});
