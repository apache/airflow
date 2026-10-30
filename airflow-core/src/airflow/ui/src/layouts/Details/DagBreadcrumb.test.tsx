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
import { cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { MemoryRouter, Route, Routes, useLocation, useMatches } from "react-router-dom";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { DagRunService, DagService, TaskInstanceService } from "openapi/requests/services.gen";

import { BaseWrapper } from "src/utils/Wrapper";

import { DagBreadcrumb } from "./DagBreadcrumb";

// The Dag search reads the active tab from the router's matches, which needs a data router; these
// tests mount a plain MemoryRouter, so stub it out. `mockReset` clears the implementation between
// tests, hence the beforeEach rather than a default in the factory.
vi.mock("react-router-dom", async (importOriginal) => {
  const original = await importOriginal();

  return { ...(original as object), useMatches: vi.fn() };
});

const LOOP_PASSES = [
  { iteration: undefined, iterationsRan: 3, loopId: "batch", loopRegionId: "r", maxIterations: 5 },
  { iteration: 2, loopId: "batch", loopRegionId: "r" },
  { iteration: 1, loopId: "batch", loopRegionId: "r" },
  { iteration: 0, loopId: "batch", loopRegionId: "r" },
];

// Most levels have no loop in scope; the loop tests opt in by setting this.
let mockLoopOptions: Array<unknown> = [];

vi.mock("src/queries/useLoopFilterOptions", async (importOriginal) => {
  const original = await importOriginal();

  return { ...(original as object), useLoopFilterOptions: () => ({ options: mockLoopOptions }) };
});

// The msw dag-details handler is pinned to this dag id.
const DAG_ID = "tutorial_taskflow_api";

const LocationProbe = () => <div data-testid="location-pathname">{useLocation().pathname}</div>;

const createWrapper =
  (path: string, route: string) =>
  ({ children }: PropsWithChildren) => (
    <BaseWrapper>
      <MemoryRouter initialEntries={[path]}>
        <Routes>
          <Route
            element={
              <>
                {children}
                <LocationProbe />
              </>
            }
            path={route}
          />
        </Routes>
      </MemoryRouter>
    </BaseWrapper>
  );

beforeEach(() => {
  mockLoopOptions = [];
  vi.mocked(useMatches).mockReturnValue([]);
});

afterEach(() => cleanup());

describe("DagBreadcrumb", () => {
  it("never renders the pause toggle at the Dag level", async () => {
    render(<DagBreadcrumb />, { wrapper: createWrapper(`/dags/${DAG_ID}`, "/dags/:dagId") });

    await screen.findByRole("link", { name: new RegExp(DAG_ID, "u") });

    expect(screen.queryByTestId("toggle-pause")).not.toBeInTheDocument();
  });

  it("links the Dag level to the Dag page and offers the switcher", async () => {
    render(<DagBreadcrumb />, { wrapper: createWrapper(`/dags/${DAG_ID}`, "/dags/:dagId") });

    expect(await screen.findByRole("link", { name: new RegExp(DAG_ID, "u") })).toHaveAttribute(
      "href",
      `/dags/${DAG_ID}`,
    );
    expect(screen.getByTestId("switch-dag")).toBeInTheDocument();
  });

  it("keeps the state badge's room while the state is still loading", async () => {
    vi.spyOn(DagRunService, "getDagRun").mockReturnValue(
      new Promise(() => undefined) as ReturnType<typeof DagRunService.getDagRun>,
    );

    render(<DagBreadcrumb />, {
      wrapper: createWrapper(`/dags/${DAG_ID}/runs/run_1`, "/dags/:dagId/runs/:runId"),
    });

    await screen.findByRole("link", { name: new RegExp(DAG_ID, "u") });

    // Nothing resolves this run, so its state never arrives. The badge still has to be in the DOM,
    // or the bar would change width the moment it did.
    expect(screen.getAllByTestId("state-badge")).toHaveLength(1);
  });

  it("gives no badge room to levels that have no state", async () => {
    render(<DagBreadcrumb />, { wrapper: createWrapper(`/dags/${DAG_ID}`, "/dags/:dagId") });

    await screen.findByRole("link", { name: new RegExp(DAG_ID, "u") });

    expect(screen.queryByTestId("state-badge")).toBeNull();
  });

  it("switches Dag from the chevron dropdown", async () => {
    render(<DagBreadcrumb />, { wrapper: createWrapper(`/dags/${DAG_ID}`, "/dags/:dagId") });

    fireEvent.click(await screen.findByTestId("switch-dag"));

    const option = await screen.findByText(`${DAG_ID}_success`, undefined, { timeout: 3000 });

    fireEvent.click(option);

    await waitFor(() =>
      expect(screen.getByTestId("location-pathname")).toHaveTextContent(`/dags/${DAG_ID}_success`),
    );
  });

  it("keeps a run level on a Dag page with no run, so a run can be picked from anywhere", async () => {
    render(<DagBreadcrumb />, { wrapper: createWrapper(`/dags/${DAG_ID}`, "/dags/:dagId") });

    expect(await screen.findByRole("link", { name: /allRuns/u })).toHaveAttribute(
      "href",
      `/dags/${DAG_ID}/runs`,
    );
    expect(screen.getByTestId("switch-dag-run")).toBeInTheDocument();
  });

  it("loads what both dropdowns list before either one is opened", async () => {
    const getDagsUi = vi.spyOn(DagService, "getDagsUi").mockResolvedValue({ dags: [], total_entries: 0 });
    const getDagRuns = vi
      .spyOn(DagRunService, "getDagRuns")
      .mockResolvedValue({ dag_runs: [], total_entries: 0 });

    render(<DagBreadcrumb />, { wrapper: createWrapper(`/dags/${DAG_ID}`, "/dags/:dagId") });

    await waitFor(() => expect(getDagsUi).toHaveBeenCalled());
    await waitFor(() =>
      expect(getDagRuns).toHaveBeenCalledWith(
        expect.objectContaining({ dagId: DAG_ID, limit: 10, orderBy: ["-run_after"] }),
      ),
    );

    // Neither panel was ever opened: what they list is loaded by the breadcrumb level itself, so
    // opening one is not a round trip and never starts out empty.
    expect(screen.queryByTestId("switch-dag-popover")).not.toBeInTheDocument();
    expect(screen.queryByTestId("switch-dag-run-popover")).not.toBeInTheDocument();
  });

  it("asks for fresh runs when the panel opens, having listed them without polling for them", async () => {
    const getDagRuns = vi
      .spyOn(DagRunService, "getDagRuns")
      .mockResolvedValue({ dag_runs: [], total_entries: 0 });

    render(<DagBreadcrumb />, { wrapper: createWrapper(`/dags/${DAG_ID}`, "/dags/:dagId") });

    const listCalls = () =>
      getDagRuns.mock.calls.filter(([params]) => params.limit === 10 && params.orderBy !== undefined).length;

    // Loaded up front, so the panel never opens onto an empty list.
    await waitFor(() => expect(listCalls()).toBe(1));

    fireEvent.click(await screen.findByTestId("switch-dag-run"));

    // Opening catches up whatever changed while the panel was closed and not polling.
    await waitFor(() => expect(listCalls()).toBe(2));
  });

  it("switches Dag run from the chevron dropdown, keeping the task in view", async () => {
    render(<DagBreadcrumb />, {
      wrapper: createWrapper(
        `/dags/${DAG_ID}/runs/run_1/tasks/task_1`,
        "/dags/:dagId/runs/:runId/tasks/:taskId",
      ),
    });

    fireEvent.click(await screen.findByTestId("switch-dag-run"));

    fireEvent.click(await screen.findByText("run_in_range", undefined, { timeout: 3000 }));

    await waitFor(() =>
      expect(screen.getByTestId("location-pathname")).toHaveTextContent(
        `/dags/${DAG_ID}/runs/run_in_range/tasks/task_1`,
      ),
    );
  });

  it("offers the instance search at the map index level", async () => {
    render(<DagBreadcrumb />, {
      wrapper: createWrapper(
        `/dags/${DAG_ID}/runs/run_1/tasks/task_1/mapped/0`,
        "/dags/:dagId/runs/:runId/tasks/:taskId/mapped/:mapIndex",
      ),
    });

    expect(await screen.findByTestId("switch-map-index")).toBeInTheDocument();
  });

  it("keeps the map index level on the expanded task's summary, so an instance is one click away", async () => {
    render(<DagBreadcrumb />, {
      wrapper: createWrapper(
        `/dags/${DAG_ID}/runs/run_1/tasks/task_1/mapped`,
        "/dags/:dagId/runs/:runId/tasks/:taskId/mapped",
      ),
    });

    // No instance is picked here, so the level stands on "all" rather than dropping off the bar.
    expect(await screen.findByText("allMapIndexes")).toBeInTheDocument();
    expect(screen.getByTestId("switch-map-index")).toBeInTheDocument();
  });

  it("renders one level per URL segment, with only the current one unlinked", async () => {
    render(<DagBreadcrumb />, {
      wrapper: createWrapper(
        `/dags/${DAG_ID}/runs/run_1/tasks/task_1/mapped/0`,
        "/dags/:dagId/runs/:runId/tasks/:taskId/mapped/:mapIndex",
      ),
    });

    await screen.findByRole("link", { name: new RegExp(DAG_ID, "u") });

    for (const caption of ["dag_one", "dagRun_one", "taskInstance_other", "mapIndex"]) {
      expect(screen.getByText(caption)).toBeInTheDocument();
    }

    // The caption sits inside the link, so it is part of the accessible name too.
    expect(screen.getByRole("link", { name: /run_1/u })).toHaveAttribute(
      "href",
      `/dags/${DAG_ID}/runs/run_1`,
    );
    // A map index in the URL means the instances are expanded, so the task level links to the list
    // that sits between the run and this instance — even for a task whose own `is_mapped` is false,
    // as happens inside a mapped task group.
    expect(screen.getByRole("link", { name: /task_1/u })).toHaveAttribute(
      "href",
      `/dags/${DAG_ID}/runs/run_1/tasks/task_1/mapped`,
    );
    // The map index names itself but is not a link: every destination it could have is this page,
    // and the trip would drop the region the url carries.
    expect(screen.queryByRole("link", { name: /mapIndex/u })).not.toBeInTheDocument();
    expect(screen.getByText("0")).toBeInTheDocument();
  });

  it("resolves the task instance by the region coordinates in the URL", async () => {
    const regionId = "11111111-1111-4111-8111-111111111111";
    const getTaskInstance = vi.spyOn(TaskInstanceService, "getTaskInstance");

    render(<DagBreadcrumb />, {
      wrapper: createWrapper(
        `/dags/${DAG_ID}/runs/run_1/tasks/task_1?region_id=${regionId}&region_index=2`,
        "/dags/:dagId/runs/:runId/tasks/:taskId",
      ),
    });

    await waitFor(() =>
      expect(getTaskInstance).toHaveBeenCalledWith(
        expect.objectContaining({ regionId, regionIndex: 2, taskId: "task_1" }),
      ),
    );
  });

  it("names the newest pass when the url pins none", async () => {
    mockLoopOptions = LOOP_PASSES;
    render(<DagBreadcrumb />, {
      wrapper: createWrapper(
        `/dags/${DAG_ID}/runs/run_1/tasks/batch.member`,
        "/dags/:dagId/runs/:runId/tasks/:taskId",
      ),
    });

    // Passes arrive newest first, so the highest index leads.
    expect(await screen.findByTestId("switch-iteration")).toBeInTheDocument();
    expect(screen.getByTestId("dag-breadcrumb")).toHaveTextContent("2");
  });

  it("falls back to this loop's newest when the url carries another loop's pass", async () => {
    mockLoopOptions = LOOP_PASSES;
    render(<DagBreadcrumb />, {
      wrapper: createWrapper(
        `/dags/${DAG_ID}/runs/run_1/tasks/batch.member?loop_id=other&iteration=0`,
        "/dags/:dagId/runs/:runId/tasks/:taskId",
      ),
    });

    await screen.findByTestId("switch-iteration");

    // Reading "0" here would name a pass of a loop that never counted it.
    expect(screen.getByTestId("dag-breadcrumb")).toHaveTextContent("2");
  });
});
