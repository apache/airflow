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
import { fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import { delay, http, HttpResponse } from "msw";
import { setupServer, type SetupServer } from "msw/node";
import { afterAll, afterEach, beforeAll, describe, expect, it } from "vitest";

import type { DagSchedulingState } from "openapi/requests/types.gen";

import { DAGS_LIST_DISPLAY_KEY, DAGS_LIST_SHOW_RECENT_TASKS_KEY } from "src/constants/localStorage";
import { handlers } from "src/mocks/handlers";
import { failedDag, pausedDag, successDag } from "src/mocks/handlers/dags";
import { AppWrapper } from "src/utils/AppWrapper";

let server: SetupServer;

beforeAll(() => {
  server = setupServer(...handlers);
  server.listen({ onUnhandledFrame: "bypass" });
});

afterEach(() => {
  server.resetHandlers();
  localStorage.clear();
});
afterAll(() => server.close());

describe("Dag Filters", () => {
  it("passes an exact scheduling state from the URL to the API", async () => {
    let requestedSchedulingState: string | null = null;

    server.use(
      http.get("/ui/dags", ({ request }) => {
        requestedSchedulingState = new URL(request.url).searchParams.get("scheduling_state");

        return HttpResponse.json({ dags: [], total_entries: 0 });
      }),
    );

    render(<AppWrapper initialEntries={["/dags?scheduling_state=active"]} />);

    await waitFor(() => expect(requestedSchedulingState).toBe("active"));
    expect(await screen.findByTestId("scheduling_state-pill")).toHaveTextContent("schedulingState.active");
  });

  it("Filter by selected run state, matching the latest run by default", async () => {
    render(<AppWrapper initialEntries={["/dags"]} />);

    await waitFor(() => expect(screen.getByText("tutorial_taskflow_api_success")).toBeInTheDocument());

    fireEvent.click(screen.getByTestId("add-filter-button"));
    fireEvent.click(await screen.findByTestId("add-filter-run_state"));

    // A newly added select opens straight onto its options, so there is no trigger to click.
    await waitFor(() => screen.getByTestId("run_state-filter-success").click());
    await waitFor(() => expect(screen.getByText("tutorial_taskflow_api_success")).toBeInTheDocument());

    fireEvent.click(await screen.findByTestId("run_state-pill"));
    await waitFor(() => screen.getByTestId("run_state-filter").click());
    await waitFor(() => screen.getByTestId("run_state-filter-failed").click());
    await waitFor(() => expect(screen.getByText("tutorial_taskflow_api_failed")).toBeInTheDocument());
  });

  it("Widens the run state filter to a time lookback", async () => {
    render(<AppWrapper initialEntries={["/dags?last_dag_run_state=failed"]} />);

    await waitFor(() => expect(screen.getByText("tutorial_taskflow_api_failed")).toBeInTheDocument());
    expect(screen.queryByText("tutorial_taskflow_api_success")).not.toBeInTheDocument();

    fireEvent.click(await screen.findByTestId("run_state-pill"));
    await waitFor(() => screen.getByTestId("run_state-lookback").click());
    await waitFor(() => screen.getByTestId("run_state-lookback-168").click());

    // The mock treats every Dag as having some failed run within the window.
    await waitFor(() => expect(screen.getByText("tutorial_taskflow_api_success")).toBeInTheDocument());
    expect(screen.getByText("tutorial_taskflow_api_failed")).toBeInTheDocument();
  });

  it("Restores the run state pill from any-run URL params", async () => {
    render(<AppWrapper initialEntries={["/dags?dag_run_state=failed&dag_run_state_within_hours=168"]} />);

    const pill = await screen.findByTestId("run_state-pill");

    expect(pill).toHaveTextContent("filters.runState");
  });

  it("keeps the listed Dags on screen while a newly added filter is still loading", async () => {
    render(<AppWrapper initialEntries={["/dags"]} />);

    await waitFor(() => expect(screen.getByText("tutorial_taskflow_api_failed")).toBeInTheDocument());

    let refetchStarted = false;

    server.use(
      http.get("/ui/dags", async () => {
        refetchStarted = true;
        await delay("infinite");

        return HttpResponse.json({ dags: [], total_entries: 0 });
      }),
    );

    fireEvent.click(screen.getByTestId("add-filter-button"));
    fireEvent.click(await screen.findByTestId("add-filter-run_state"));
    await waitFor(() => screen.getByTestId("run_state-filter-success").click());

    await waitFor(() => {
      expect(screen.getByTestId("run_state-pill")).toBeInTheDocument();
      expect(refetchStarted).toBe(true);
    });

    expect(screen.getByText("tutorial_taskflow_api_failed")).toBeInTheDocument();
    expect(screen.queryAllByTestId("skeleton")).toHaveLength(0);
  });
});

type BulkRequestBody = {
  actions: Array<{ entities: Array<{ dag_id: string; scheduling_state: string }> }>;
};

const renderDagsAndSelectAll = async (
  dags: Array<{ scheduling_state?: DagSchedulingState } & typeof successDag>,
) => {
  const captured: { body?: BulkRequestBody } = {};

  server.use(
    http.get("/ui/dags", () => HttpResponse.json({ dags, total_entries: dags.length })),
    http.patch("/api/v2/dags/bulk", async ({ request }) => {
      captured.body = (await request.json()) as BulkRequestBody;

      return HttpResponse.json({
        update: { errors: [], success: dags.map((dag) => dag.dag_id) },
      });
    }),
  );

  localStorage.setItem(DAGS_LIST_DISPLAY_KEY, JSON.stringify("table"));
  render(<AppWrapper initialEntries={["/dags"]} />);

  await waitFor(() => expect(screen.getByText(dags[0]?.dag_display_name ?? "")).toBeInTheDocument());

  fireEvent.click(within(screen.getByTestId("table-list")).getAllByRole("checkbox")[0] as HTMLInputElement);

  return captured;
};

const getRequestedSchedulingStates = async (captured: { body?: BulkRequestBody }) => {
  await waitFor(() => expect(captured.body).toBeDefined());

  return Object.fromEntries(
    (captured.body?.actions[0]?.entities ?? []).map((entity) => [entity.dag_id, entity.scheduling_state]),
  );
};

describe("Bulk pause/drain Dags", () => {
  it.each([
    { pausedDagHasUnfinishedRuns: false, scenario: "no selected Dag has unfinished runs" },
    { pausedDagHasUnfinishedRuns: true, scenario: "only an already-paused Dag has unfinished runs" },
  ])(
    "skips the drain choice and pauses every selected Dag when $scenario",
    async ({ pausedDagHasUnfinishedRuns }) => {
      const captured = await renderDagsAndSelectAll([
        successDag,
        failedDag,
        { ...pausedDag, has_unfinished_runs: pausedDagHasUnfinishedRuns },
      ]);

      fireEvent.click(await screen.findByTestId("bulk-pause-drain-dags"));

      const confirmButton = await screen.findByTestId("confirmation-confirm-button");

      expect(screen.queryByTestId("drain-dag")).not.toBeInTheDocument();
      fireEvent.click(confirmButton);

      expect(await getRequestedSchedulingStates(captured)).toEqual({
        paused_dag: "paused",
        tutorial_taskflow_api_failed: "paused",
        tutorial_taskflow_api_success: "paused",
      });
    },
  );

  it.each([
    { choice: "drain-dag", expectedState: "draining" },
    { choice: "pause-dag-now", expectedState: "paused" },
  ])(
    "offers the drain choice once for a mix of idle and running Dags and applies $choice to the unpaused ones",
    async ({ choice, expectedState }) => {
      const captured = await renderDagsAndSelectAll([
        { ...successDag, has_unfinished_runs: true },
        failedDag,
        { ...pausedDag, has_unfinished_runs: true },
      ]);

      fireEvent.click(await screen.findByTestId("bulk-pause-drain-dags"));

      const choiceButton = await screen.findByTestId(choice);

      expect(screen.queryByTestId("confirmation-confirm-button")).not.toBeInTheDocument();
      fireEvent.click(choiceButton);

      expect(await getRequestedSchedulingStates(captured)).toEqual({
        paused_dag: "paused",
        tutorial_taskflow_api_failed: expectedState,
        tutorial_taskflow_api_success: expectedState,
      });
    },
  );

  it("unpauses every selected Dag, including cancelling a drain, in one bulk request", async () => {
    const captured = await renderDagsAndSelectAll([
      successDag,
      { ...failedDag, scheduling_state: "draining" },
      pausedDag,
    ]);

    fireEvent.click(await screen.findByTestId("bulk-unpause-dags"));
    fireEvent.click(await screen.findByTestId("confirmation-confirm-button"));

    expect(await getRequestedSchedulingStates(captured)).toEqual({
      paused_dag: "active",
      tutorial_taskflow_api_failed: "active",
      tutorial_taskflow_api_success: "active",
    });
  });

  it.each([
    {
      dags: [pausedDag],
      pauseDisabled: true,
      scenario: "every selected Dag is paused",
      unpauseDisabled: false,
    },
    {
      dags: [successDag, failedDag],
      pauseDisabled: false,
      scenario: "every selected Dag is active",
      unpauseDisabled: true,
    },
    {
      dags: [{ ...failedDag, scheduling_state: "draining" as const }],
      pauseDisabled: false,
      scenario: "a selected Dag is draining",
      unpauseDisabled: false,
    },
  ])(
    "disables the bulk actions that would change nothing when $scenario",
    async ({ dags, pauseDisabled, unpauseDisabled }) => {
      await renderDagsAndSelectAll(dags);

      expect(await screen.findByTestId("bulk-pause-drain-dags")).toHaveProperty("disabled", pauseDisabled);
      expect(screen.getByTestId("bulk-unpause-dags")).toHaveProperty("disabled", unpauseDisabled);
    },
  );
});

describe("Dag sorting", () => {
  it("sorts cards by latest run after", async () => {
    render(<AppWrapper initialEntries={["/dags"]} />);

    await waitFor(() => expect(screen.getByText("tutorial_taskflow_api_success")).toBeInTheDocument());

    const trigger = within(screen.getByTestId("sort-by-select")).getByRole("combobox");

    await waitFor(() => trigger.click());
    await waitFor(() => screen.getByText("sort.lastRunAfter.desc").click());

    await waitFor(() =>
      expect(screen.getAllByText(/tutorial_taskflow_api_/u)[0]).toHaveTextContent(
        "tutorial_taskflow_api_failed",
      ),
    );
  });

  it("sorts the latest run column by run after", async () => {
    localStorage.setItem(DAGS_LIST_DISPLAY_KEY, JSON.stringify("table"));
    render(<AppWrapper initialEntries={["/dags"]} />);

    await waitFor(() => expect(screen.getByTestId("table-list")).toBeInTheDocument());

    screen.getByText("dagDetails.latestRun").closest("button")?.click();

    await waitFor(() =>
      expect(screen.getAllByTestId("table-cell-dag_display_name")[0]).toHaveTextContent(
        "tutorial_taskflow_api_failed",
      ),
    );
  });

  it("adds a secondary sort on shift-click and sends every sort to the request", async () => {
    localStorage.setItem(DAGS_LIST_DISPLAY_KEY, JSON.stringify("table"));
    const requestedOrderBy: Array<Array<string>> = [];

    server.use(
      http.get("/ui/dags", ({ request }) => {
        requestedOrderBy.push(new URL(request.url).searchParams.getAll("order_by"));

        return HttpResponse.json({ dags: [], total_entries: 0 });
      }),
    );
    render(<AppWrapper initialEntries={["/dags?sort=-last_run_run_after"]} />);

    await waitFor(() => expect(screen.getByTestId("table-list")).toBeInTheDocument());
    await waitFor(() => expect(requestedOrderBy.at(-1)).toEqual(["-last_run_run_after"]));

    fireEvent.click(screen.getByText("dagId").closest("button") as HTMLButtonElement, { shiftKey: true });

    await waitFor(() => expect(requestedOrderBy.at(-1)).toEqual(["-last_run_run_after", "dag_display_name"]));
    expect(screen.getByTestId("sort-index-last_run_run_after")).toHaveTextContent("1");
    expect(screen.getByTestId("sort-index-dag_display_name")).toHaveTextContent("2");
  });
});

describe("Recent tasks setting", () => {
  it.each([true, false])("fetches and shows the counts only when the setting is %s", async (show) => {
    let countsRequests = 0;
    let runStateCountsRequests = 0;

    server.use(
      http.get("/ui/dags/recent_task_instance_state_counts", () => {
        countsRequests += 1;

        return HttpResponse.json({ dags: [] });
      }),
      http.get("/ui/dags/run_state_counts", () => {
        runStateCountsRequests += 1;

        return HttpResponse.json({ dags: [] });
      }),
    );
    localStorage.setItem(DAGS_LIST_DISPLAY_KEY, JSON.stringify("table"));
    localStorage.setItem(DAGS_LIST_SHOW_RECENT_TASKS_KEY, JSON.stringify(show));
    render(<AppWrapper initialEntries={["/dags"]} />);

    // The run state counts are requested from the same Dag list response, so once that request
    // has arrived, a task state counts request would have been sent too.
    await waitFor(() => expect(runStateCountsRequests).toBeGreaterThan(0));

    if (show) {
      await waitFor(() => expect(countsRequests).toBeGreaterThan(0));
      expect(screen.getAllByTestId("table-cell-recent_task_state_counts").length).toBeGreaterThan(0);
      expect(screen.getByTestId("recent-task-state-counts-info")).toBeInTheDocument();
    } else {
      expect(countsRequests).toBe(0);
      expect(screen.queryByTestId("table-cell-recent_task_state_counts")).not.toBeInTheDocument();
    }
  });
});

describe("Dags table", () => {
  it.each([
    "dag_display_name",
    "timetable_description",
    "next_dagrun",
    "last_run_run_after",
    "tags",
    "team_name",
  ])("keeps the %s cell on one line", async (columnId) => {
    server.use(
      http.get("/ui/config", () => HttpResponse.json({ multi_team: true, page_size: 15 })),
      http.get("/ui/dags", () =>
        HttpResponse.json({
          dags: [
            {
              ...successDag,
              next_dagrun_run_after: "2025-01-14T00:00:00Z",
              team_name: "team_a",
              timetable_summary: "@daily",
            },
          ],
          total_entries: 1,
        }),
      ),
    );
    localStorage.setItem(DAGS_LIST_DISPLAY_KEY, JSON.stringify("table"));
    render(<AppWrapper initialEntries={["/dags"]} />);

    const cell = await screen.findByTestId(`table-cell-${columnId}`);

    await waitFor(() => expect(cell.firstElementChild).toHaveStyle({ whiteSpace: "nowrap" }));
  });
});

describe("Dags display toggle", () => {
  it("switches between card and table views", async () => {
    render(<AppWrapper initialEntries={["/dags"]} />);

    const cardList = await screen.findByTestId("card-list");

    await waitFor(() =>
      expect(within(cardList).getByText("tutorial_taskflow_api_success")).toBeInTheDocument(),
    );
    expect(screen.queryByTestId("table-list")).toBeNull();

    fireEvent.click(screen.getByLabelText(/toggleTableView/iu));

    const tableList = await screen.findByTestId("table-list");

    expect(within(tableList).getByText("tutorial_taskflow_api_success")).toBeInTheDocument();
    expect(screen.queryByTestId("card-list")).toBeNull();

    fireEvent.click(screen.getByLabelText(/toggleCardView/iu));

    await waitFor(() =>
      expect(
        within(screen.getByTestId("card-list")).getByText("tutorial_taskflow_api_success"),
      ).toBeInTheDocument(),
    );
    expect(screen.queryByTestId("table-list")).toBeNull();
  });
});
