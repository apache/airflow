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
import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import { Link, MemoryRouter, Route, Routes } from "react-router-dom";
import { afterEach, beforeAll, describe, expect, it, vi } from "vitest";

import { DagRunService, TaskInstanceService } from "openapi/requests";
import type {
  ExecutionCollectionResponse,
  ExecutionRegionResponse,
  ExecutionTaskResponse,
} from "openapi/requests/types.gen";

import i18n from "src/i18n/config";
import type * as Utils from "src/utils";
import { BaseWrapper } from "src/utils/Wrapper";

import dagTranslations from "../../../public/i18n/locales/en/dag.json";
import { Execution } from "./Execution";

vi.mock("src/router", () => ({ taskInstanceRoutes: [] }));

beforeAll(() => {
  i18n.addResourceBundle("en", "dag", dagTranslations, true, true);
});

vi.mock("src/utils", async (importOriginal) => ({
  ...(await importOriginal<typeof Utils>()),
  useAutoRefresh: () => false,
}));

const ROOT = "11111111-1111-4111-8111-111111111111";
const FORK = "22222222-2222-4222-8222-222222222222";
const MAPPING = "33333333-3333-4333-8333-333333333333";

const region = (id: string, changes: Partial<ExecutionRegionResponse> = {}): ExecutionRegionResponse => ({
  forked_from_region_id: null,
  id,
  node_id: "body",
  parent_region_id: null,
  parent_region_index: null,
  resumes_from_index: 0,
  ...changes,
});

const task = (id: string, changes: Partial<ExecutionTaskResponse> = {}): ExecutionTaskResponse => ({
  dag_id: "dag",
  dag_run_id: "run",
  dag_version_id: null,
  duration: null,
  end_date: null,
  id,
  map_index: -1,
  operator: "EmptyOperator",
  region_id: ROOT,
  region_index: 0,
  start_date: null,
  state: "success",
  task_display_name: id,
  task_id: id,
  try_number: 1,
  ...changes,
});

const renderExecution = (search = "") =>
  render(
    <BaseWrapper>
      <MemoryRouter initialEntries={[`/dags/dag/runs/run/execution${search}`]}>
        <Routes>
          <Route element={<Execution />} path="/dags/:dagId/runs/:runId/execution" />
        </Routes>
      </MemoryRouter>
    </BaseWrapper>,
  );

afterEach(() => vi.restoreAllMocks());

describe("Run Execution", () => {
  it("resets selected UUIDs when navigating to another run", async () => {
    vi.spyOn(DagRunService, "getExecution").mockResolvedValue({
      regions: [region(ROOT)],
      task_instances: [task("work")],
      total_entries: 1,
    });
    render(
      <BaseWrapper>
        <MemoryRouter initialEntries={["/dags/dag/runs/first/execution"]}>
          <Link to="/dags/dag/runs/second/execution">Second run</Link>
          <Routes>
            <Route element={<Execution />} path="/dags/:dagId/runs/:runId/execution" />
          </Routes>
        </MemoryRouter>
      </BaseWrapper>,
    );
    fireEvent.click(await screen.findByRole("checkbox", { name: "Select work" }));
    await waitFor(() =>
      expect(screen.getByRole("button", { name: "Clear selected executions" })).toBeEnabled(),
    );
    fireEvent.click(screen.getByRole("link", { name: "Second run" }));
    expect(screen.getByRole("button", { name: "Clear selected executions" })).toBeDisabled();
  });
  it("clears selected UUIDs separately when public task coordinates collide", async () => {
    vi.spyOn(DagRunService, "getExecution").mockResolvedValue({
      regions: [region(ROOT), region(FORK)],
      task_instances: [
        task("first", { task_id: "work" }),
        task("second", { region_id: FORK, region_index: 1, task_id: "work" }),
      ],
      total_entries: 2,
    });
    const clear = vi
      .spyOn(TaskInstanceService, "postClearTaskInstances")
      .mockResolvedValue({ task_instances: [], total_entries: 0 });

    renderExecution();
    fireEvent.click(await screen.findByRole("checkbox", { name: "Select first" }));
    fireEvent.click(screen.getByRole("checkbox", { name: "Select second" }));
    await waitFor(() =>
      expect(screen.getByRole("button", { name: "Clear selected executions" })).toBeEnabled(),
    );
    fireEvent.click(screen.getByRole("button", { name: "Clear selected executions" }));
    await waitFor(() =>
      expect(clear.mock.lastCall?.[0].requestBody).toMatchObject({
        dry_run: true,
        task_instance_ids: ["first", "second"],
      }),
    );
  });
  it("keeps running branches and retained regions visible beside newer loop passes", async () => {
    const response: ExecutionCollectionResponse = {
      regions: [region(ROOT), region(FORK, { forked_from_region_id: ROOT, resumes_from_index: 2 })],
      task_instances: [
        task("early", { state: "running" }),
        task("retained", { region_index: 3 }),
        task("gate", { operator: "LoopGateOperator", region_id: FORK, region_index: 2 }),
      ],
      total_entries: 3,
    };

    vi.spyOn(DagRunService, "getExecution").mockResolvedValue(response);
    renderExecution();

    expect(await screen.findByRole("heading", { name: "body · Iteration 0" })).toBeVisible();
    expect(screen.getByRole("heading", { name: "body · Iteration 2" })).toBeVisible();
    expect(screen.getByRole("heading", { name: "body · Iteration 3" })).toBeVisible();
    const gate = screen.getByRole("link", { name: "gate" });

    expect(gate).toHaveAttribute("href", expect.stringContaining(`region_id=${FORK}`));
    expect(gate).toHaveAttribute("href", expect.stringContaining("region_index=2"));
    expect(gate).toHaveAttribute("href", expect.stringContaining("try_number=1"));
    expect(screen.getByRole("link", { name: "early" })).toBeVisible();
    expect(screen.queryByRole("heading", { name: "body · Iteration 4" })).not.toBeInTheDocument();
  });

  it.each(["body.map", "body.mapped.member"])(
    "expands %s positions within loop iteration 3 and preserves skipped empty mapping",
    async (taskId) => {
      vi.spyOn(DagRunService, "getExecution").mockResolvedValue({
        regions: [
          region(ROOT),
          region(MAPPING, { node_id: taskId, parent_region_id: ROOT, parent_region_index: 3 }),
          region("empty-mapping", { node_id: "empty", parent_region_id: ROOT, parent_region_index: 3 }),
        ],
        task_instances: [
          task("map-zero", {
            map_index: 0,
            region_id: MAPPING,
            region_index: 0,
            task_display_name: "map",
            task_id: taskId,
          }),
          task("map-one", {
            map_index: 1,
            region_id: MAPPING,
            region_index: 1,
            task_display_name: "map",
            task_id: taskId,
          }),
          task("empty", { region_id: "empty-mapping", region_index: -1, state: "skipped" }),
        ],
        total_entries: 3,
      });
      renderExecution();

      fireEvent.click(await screen.findByRole("button", { name: "map (2 mapped tasks on this page)" }));
      expect(screen.getByRole("link", { name: "map [0]" })).toHaveAttribute(
        "href",
        expect.stringContaining("/mapped/0/logs"),
      );
      expect(screen.getByRole("link", { name: "map [1]" })).toHaveAttribute(
        "href",
        expect.stringContaining("region_index=1"),
      );
      expect(screen.getByRole("link", { name: "empty" })).toBeVisible();
      expect(screen.getByRole("heading", { name: "body · Iteration 3" })).toBeVisible();
    },
  );

  it("pages using the API total without presenting a page as the complete loop", async () => {
    const fetch = vi
      .spyOn(DagRunService, "getExecution")
      .mockResolvedValueOnce({
        regions: [region(ROOT)],
        task_instances: [task("first")],
        total_entries: 101,
      })
      .mockResolvedValueOnce({
        regions: [region(ROOT)],
        task_instances: [task("second")],
        total_entries: 101,
      });

    renderExecution();
    expect(await screen.findByText("Showing 1–1 of 101 executions")).toBeVisible();
    fireEvent.click(screen.getByRole("button", { name: "Next page" }));
    await waitFor(() =>
      expect(fetch).toHaveBeenLastCalledWith(expect.objectContaining({ limit: 100, offset: 100 })),
    );
    expect(await screen.findByRole("link", { name: "second" })).toBeVisible();
    expect(screen.queryByRole("link", { name: "first" })).not.toBeInTheDocument();
    expect(screen.getByText("Showing 101–101 of 101 executions")).toBeVisible();
  });
});
