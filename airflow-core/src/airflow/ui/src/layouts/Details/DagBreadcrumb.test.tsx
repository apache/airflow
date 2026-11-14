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
import { cleanup, fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import { http, HttpResponse } from "msw";
import { MemoryRouter, Route, Routes, matchRoutes, useLocation, useMatches } from "react-router-dom";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { DagRunService, DagService, GridService, TaskInstanceService } from "openapi/requests/services.gen";

import { StateIcon } from "src/components/StateIcon";

import { server } from "src/mocks/server";
import { routerConfig } from "src/router";
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

// Empty unless a test gives the run on screen some task instance states.
let mockSummaries = new Map<string, unknown>();

vi.mock("src/queries/useGridTISummaries.ts", () => ({
  useGridTiSummariesStream: () => ({ summariesByRunId: mockSummaries }),
}));

vi.mock("src/queries/useLoopFilterOptions", async (importOriginal) => {
  const original = await importOriginal();

  return { ...(original as object), useLoopFilterOptions: () => ({ options: mockLoopOptions }) };
});

// The msw dag-details handler is pinned to this dag id.
const DAG_ID = "tutorial_taskflow_api";

const leadsToErrorPage = (href: string) => matchRoutes(routerConfig, href)?.at(-1)?.route.path === "*";

const LocationProbe = () => {
  const { pathname, search } = useLocation();

  return (
    <>
      <div data-testid="location-pathname">{pathname}</div>
      <div data-testid="location-search">{search}</div>
    </>
  );
};

const TASK_PATH = `/dags/${DAG_ID}/runs/run_1/tasks/task_1`;
const TASK_ROUTE = "/dags/:dagId/runs/:runId/tasks/:taskId";

// What the grid structure says, which is the only source that marks a mapped group's tasks mapped.
const STRUCTURE = [
  { children: null, id: "task_1", is_mapped: false, label: "task_1" },
  { children: null, id: "load_orders", is_mapped: false, label: "Load orders" },
  { children: null, id: "load_users", is_mapped: true, label: "Load users" },
  {
    children: [{ children: null, id: "per_region.fetch", is_mapped: true, label: "Fetch region" }],
    id: "per_region",
    is_mapped: true,
    label: "per_region",
  },
  {
    children: [{ children: null, id: "accumulate.increment", is_mapped: false, label: "Increment counter" }],
    id: "accumulate",
    is_loop: true,
    is_mapped: false,
    label: "accumulate",
  },
];

const buildRunSummaries = (taskInstances: Array<Record<string, unknown>>) =>
  new Map<string, unknown>([["run_1", { dag_id: DAG_ID, run_id: "run_1", task_instances: taskInstances }]]);

/** Opens the task switcher and waits for its list, returning the option named `name`. */
const openTaskSearch = async (name: RegExp = /Load orders/u) => {
  fireEvent.click(await screen.findByTestId("switch-task"));

  return screen.findByRole("option", { name });
};

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
  server.use(http.get("/ui/grid/structure/:dagId", () => HttpResponse.json(STRUCTURE)));
  mockLoopOptions = [];
  mockSummaries = new Map();
  localStorage.clear();
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

  it("loads nothing for the task switcher until it is opened", async () => {
    const getDagStructure = vi.spyOn(GridService, "getDagStructure");

    render(<DagBreadcrumb />, { wrapper: createWrapper(TASK_PATH, TASK_ROUTE) });

    // The task level's own crumb resolves through a request too, so once it shows its name the
    // page has had every chance to ask for the switcher's list.
    await screen.findByTestId("switch-task");
    await screen.findByText("task_1");

    expect(getDagStructure).not.toHaveBeenCalled();

    await openTaskSearch();

    expect(getDagStructure).toHaveBeenCalledTimes(1);
  });

  it("names the task level as the current page when it is the last one", async () => {
    render(<DagBreadcrumb />, { wrapper: createWrapper(TASK_PATH, TASK_ROUTE) });

    await screen.findByTestId("switch-task");

    expect(screen.getByText("task_1").closest("[aria-current='page']")).not.toBeNull();
  });

  it("switches task by display name prefix, keeping the run and the tab", async () => {
    render(<DagBreadcrumb />, {
      wrapper: createWrapper(`${TASK_PATH}/xcom`, `${TASK_ROUTE}/*`),
    });

    await openTaskSearch();
    fireEvent.change(screen.getByRole("combobox"), { target: { value: "LOAD O" } });

    expect(screen.queryByRole("option", { name: /Load users/u })).not.toBeInTheDocument();
    fireEvent.click(screen.getByRole("option", { name: /Load orders/u }));

    await waitFor(() =>
      expect(screen.getByTestId("location-pathname")).toHaveTextContent(
        `/dags/${DAG_ID}/runs/run_1/tasks/load_orders/xcom`,
      ),
    );
  });

  it("matches a task's display name or group anywhere once the toggle is on", async () => {
    render(<DagBreadcrumb />, { wrapper: createWrapper(TASK_PATH, TASK_ROUTE) });

    await openTaskSearch();
    fireEvent.change(screen.getByRole("combobox"), { target: { value: "region" } });

    expect(screen.queryByRole("option")).not.toBeInTheDocument();

    fireEvent.click(screen.getByTestId("advanced-search-toggle"));

    // "per_region" is the group, so its task is found by either its own name or the group's.
    expect(await screen.findByRole("option", { name: /Fetch region/u })).toHaveTextContent("per_region");
  });

  it("sends a task inside a mapped group to its expanded instances", async () => {
    render(<DagBreadcrumb />, { wrapper: createWrapper(TASK_PATH, `${TASK_ROUTE}/*`) });

    await openTaskSearch();
    fireEvent.click(screen.getByRole("option", { name: /Fetch region/u }));

    await waitFor(() =>
      expect(screen.getByTestId("location-pathname")).toHaveTextContent(
        `/dags/${DAG_ID}/runs/run_1/tasks/per_region.fetch/mapped`,
      ),
    );
  });

  it("lands a task inside a loop on its newest pass", async () => {
    mockSummaries = buildRunSummaries([
      { state: "success", task_id: "task_1" },
      {
        latest_region_id: "region-1",
        latest_region_index: 2,
        state: "success",
        task_id: "accumulate.increment",
      },
    ]);
    render(<DagBreadcrumb />, {
      wrapper: createWrapper(`${TASK_PATH}?try_number=2&dag_run_state=failed`, `${TASK_ROUTE}/*`),
    });

    await openTaskSearch(/Increment counter/u);
    fireEvent.click(screen.getByRole("option", { name: /Increment counter/u }));

    await waitFor(() =>
      expect(screen.getByTestId("location-pathname")).toHaveTextContent(
        `/dags/${DAG_ID}/runs/run_1/tasks/accumulate.increment`,
      ),
    );
    const search = new URLSearchParams(screen.getByTestId("location-search").textContent);

    expect(search.get("region_id")).toBe("region-1");
    expect(search.get("region_index")).toBe("2");
    // The try belonged to the task being left; the other filters still apply.
    expect(search.get("try_number")).toBeNull();
    expect(search.get("dag_run_state")).toBe("failed");
  });

  it("lists only the run's tasks, each with its state in that run", async () => {
    mockSummaries = buildRunSummaries([
      { state: "failed", task_id: "load_orders" },
      { state: "success", task_id: "load_users" },
    ]);
    render(<DagBreadcrumb />, { wrapper: createWrapper(TASK_PATH, TASK_ROUTE) });

    const option = await openTaskSearch();
    const { container: failedIcon } = render(<StateIcon state="failed" />);

    expect(within(option).getByTestId("state-badge").innerHTML).toBe(failedIcon.innerHTML);
    // A task the run has no instance of, such as one added in a later Dag version, would open a 404.
    expect(screen.queryByRole("option", { name: /Increment counter/u })).not.toBeInTheDocument();
  });

  it("ignores another Dag's run that shares the run id", async () => {
    mockSummaries = new Map([
      [
        "run_1",
        {
          dag_id: "other_dag",
          run_id: "run_1",
          task_instances: [{ state: "failed", task_id: "load_orders" }],
        },
      ],
    ]);
    render(<DagBreadcrumb />, { wrapper: createWrapper(TASK_PATH, TASK_ROUTE) });

    const option = await openTaskSearch();

    expect(within(option).queryByTestId("state-badge")).not.toBeInTheDocument();
  });

  it("switches task across runs on a task page with no run, keeping the tab", async () => {
    render(<DagBreadcrumb />, {
      wrapper: createWrapper(`/dags/${DAG_ID}/tasks/task_1/events`, "/dags/:dagId/tasks/:taskId/*"),
    });

    const option = await openTaskSearch();

    expect(within(option).queryByTestId("state-badge")).not.toBeInTheDocument();
    fireEvent.click(option);

    await waitFor(() =>
      expect(screen.getByTestId("location-pathname")).toHaveTextContent(
        `/dags/${DAG_ID}/tasks/load_orders/events`,
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

  it("never links a looped group page's levels to a missing page", async () => {
    mockLoopOptions = LOOP_PASSES;
    render(<DagBreadcrumb />, {
      wrapper: createWrapper(
        `/dags/${DAG_ID}/runs/run_1/tasks/group/batch`,
        "/dags/:dagId/runs/:runId/tasks/group/:groupId",
      ),
    });

    await screen.findByTestId("switch-iteration");

    for (const link of screen.getAllByRole("link")) {
      expect(leadsToErrorPage(link.getAttribute("href") ?? "")).toBe(false);
    }
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
