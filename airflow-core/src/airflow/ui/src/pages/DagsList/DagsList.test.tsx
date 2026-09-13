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
import "@testing-library/jest-dom";
import { fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import { delay, http, HttpResponse } from "msw";
import { setupServer, type SetupServer } from "msw/node";
import { afterAll, afterEach, beforeAll, describe, expect, it } from "vitest";

import { DAGS_LIST_DISPLAY_KEY } from "src/constants/localStorage";
import { handlers } from "src/mocks/handlers";
import { AppWrapper } from "src/utils/AppWrapper";

let server: SetupServer;

beforeAll(() => {
  server = setupServer(...handlers);
  server.listen({ onUnhandledRequest: "bypass" });
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

  it("Filter by selected last run state", async () => {
    render(<AppWrapper initialEntries={["/dags"]} />);

    await waitFor(() => expect(screen.getByText("tutorial_taskflow_api_success")).toBeInTheDocument());

    fireEvent.click(screen.getByTestId("add-filter-button"));
    fireEvent.click(await screen.findByTestId("add-filter-last_dag_run_state"));

    // A newly added select opens straight onto its options, so there is no trigger to click.
    await waitFor(() => screen.getByTestId("last_dag_run_state-filter-success").click());
    await waitFor(() => expect(screen.getByText("tutorial_taskflow_api_success")).toBeInTheDocument());

    fireEvent.click(await screen.findByTestId("last_dag_run_state-pill"));
    await waitFor(() => screen.getByTestId("last_dag_run_state-filter").click());
    await waitFor(() => screen.getByTestId("last_dag_run_state-filter-failed").click());
    await waitFor(() => expect(screen.getByText("tutorial_taskflow_api_failed")).toBeInTheDocument());
  });

  it("keeps the listed Dags on screen while a newly added filter is still loading", async () => {
    render(<AppWrapper initialEntries={["/dags"]} />);

    await waitFor(() => expect(screen.getByText("tutorial_taskflow_api_failed")).toBeInTheDocument());

    server.use(
      http.get("/ui/dags", async () => {
        await delay("infinite");

        return HttpResponse.json({ dags: [], total_entries: 0 });
      }),
    );

    fireEvent.click(screen.getByTestId("add-filter-button"));
    fireEvent.click(await screen.findByTestId("add-filter-last_dag_run_state"));
    await waitFor(() => screen.getByTestId("last_dag_run_state-filter-success").click());

    await waitFor(() => {
      expect(screen.getByTestId("last_dag_run_state-pill")).toBeInTheDocument();
      expect(screen.getByRole("progressbar")).toBeVisible();
    });

    expect(screen.getByText("tutorial_taskflow_api_failed")).toBeInTheDocument();
    expect(screen.queryAllByTestId("skeleton")).toHaveLength(0);
  });
});

describe("Bulk pause/drain Dags", () => {
  it("pauses every selected Dag in one bulk request", async () => {
    localStorage.setItem(DAGS_LIST_DISPLAY_KEY, JSON.stringify("table"));

    let requestBody: unknown;

    server.use(
      http.patch("/api/v2/dags/bulk", async ({ request }) => {
        requestBody = await request.json();

        return HttpResponse.json({
          update: {
            errors: [],
            success: ["tutorial_taskflow_api_success", "tutorial_taskflow_api_failed", "paused_dag"],
          },
        });
      }),
    );

    render(<AppWrapper initialEntries={["/dags"]} />);

    await waitFor(() => expect(screen.getByText("tutorial_taskflow_api_success")).toBeInTheDocument());

    // Select every row via the header checkbox, then trigger the bulk action.
    const table = screen.getByTestId("table-list");

    fireEvent.click(within(table).getAllByRole("checkbox")[0] as HTMLInputElement);
    fireEvent.click(await screen.findByTestId("bulk-pause-drain-dags"));

    // Every mocked Dag has no unfinished runs, so pausing is a single confirm — no drain choice.
    fireEvent.click(await screen.findByTestId("confirmation-confirm-button"));

    await waitFor(() => expect(requestBody).toBeDefined());
    const [action] = (
      requestBody as { actions: Array<{ entities: Array<{ dag_id: string; scheduling_state: string }> }> }
    ).actions;

    expect(action?.entities.map((entity) => entity.dag_id).sort()).toEqual(
      ["paused_dag", "tutorial_taskflow_api_failed", "tutorial_taskflow_api_success"].sort(),
    );
    expect(action?.entities.every((entity) => entity.scheduling_state === "paused")).toBe(true);
  });
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
});
