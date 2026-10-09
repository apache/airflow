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

import { TaskInstanceService } from "openapi/requests/services.gen";
import type { TaskInstanceResponse } from "openapi/requests/types.gen";

import { BaseWrapper } from "src/utils/Wrapper";
import type { MapIndexSearchOption } from "src/utils/option";

import { SearchMapIndexes } from "./SearchMapIndexes";

const { searched } = vi.hoisted<{ searched: { current: Array<MapIndexSearchOption> } }>(() => ({
  searched: { current: [] },
}));

vi.mock("chakra-react-select", () => ({
  AsyncSelect: ({
    defaultOptions,
    loadOptions,
    onChange,
  }: {
    readonly defaultOptions: Array<MapIndexSearchOption>;
    readonly loadOptions: (input: string, callback: (options: Array<MapIndexSearchOption>) => void) => void;
    readonly onChange: (option: MapIndexSearchOption) => void;
  }) => (
    <>
      {defaultOptions.map((option) => (
        <button key={option.value} onClick={() => onChange(option)} type="button">
          {`${option.label} (${option.state})`}
        </button>
      ))}
      <button
        onClick={() =>
          loadOptions("alpha", (options) => {
            searched.current = options;
          })
        }
        type="button"
      >
        Search Map Indexes
      </button>
    </>
  ),
}));

const onClose = vi.fn();

const LocationDisplay = () => <output data-testid="location">{useLocation().pathname}</output>;

const INSTANCES: Array<MapIndexSearchOption> = [
  { label: "alpha", state: "success", value: "0" },
  { label: "2", state: "failed", value: "2" },
];

const renderSearch = (initialEntry: string, route: string) =>
  render(
    <BaseWrapper>
      <MemoryRouter initialEntries={[initialEntry]}>
        <Routes>
          <Route
            element={
              <SearchMapIndexes
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
  beforeEach(() => {
    searched.current = [];
  });

  afterEach(() => vi.restoreAllMocks());

  it("names each instance by its rendered map index, with its state", () => {
    renderSearch("/dags/my_dag/runs/run_1/tasks/task_1/mapped/2", MAPPED_ROUTE);

    expect(screen.getByRole("button", { name: "alpha (success)" })).toBeInTheDocument();
    // An instance with nothing rendered still has to be nameable, so it falls back to its number.
    expect(screen.getByRole("button", { name: "2 (failed)" })).toBeInTheDocument();
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

    fireEvent.click(screen.getByRole("button", { name: "Search Map Indexes" }));

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

    fireEvent.click(screen.getByRole("button", { name: "Search Map Indexes" }));

    await waitFor(() => expect(searched.current).toEqual([{ label: "alpha", state: "running", value: "3" }]));
  });
});
