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
import { ChakraProvider, defaultSystem } from "@chakra-ui/react";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import "@testing-library/jest-dom/vitest";
import { fireEvent, render, screen, within } from "@testing-library/react";
import { http, HttpResponse } from "msw";
import { createMemoryRouter, RouterProvider } from "react-router-dom";
import { beforeAll, describe, expect, it, vi } from "vitest";

import { TimezoneProvider } from "src/context/timezone";
import { alsoWatching, triggers } from "src/mocks/handlers/triggers";
import { server } from "src/mocks/server";
import { routerConfig } from "src/router";
import { preloadAppRoutes } from "src/utils/AppWrapper";

beforeAll(() => preloadAppRoutes(["/triggers"]));

const renderAt = (path: string) =>
  render(
    <ChakraProvider value={defaultSystem}>
      <QueryClientProvider
        client={new QueryClient({ defaultOptions: { queries: { retry: false, staleTime: Infinity } } })}
      >
        <TimezoneProvider>
          <RouterProvider
            router={createMemoryRouter(
              routerConfig.map((route) => ({ ...route, loader: undefined })),
              { initialEntries: [path] },
            )}
          />
        </TimezoneProvider>
      </QueryClientProvider>
    </ChakraProvider>,
  );

// Tests render i18n keys: the msw handlers serve empty translation bundles.
describe("Triggers list", () => {
  it("shows every kind with its status and a link to what it runs for", async () => {
    renderAt("/triggers");

    // The first render loads the lazy page modules; under a full parallel run that takes over a second.
    const taskLink = await screen.findByRole("link", { name: "deferring_dag / wait" }, { timeout: 5000 });

    expect(taskLink).toHaveAttribute("href", "/dags/deferring_dag/runs/manual_run/tasks/wait");
    expect(screen.getByRole("link", { name: "deadline_dag / scheduled_run" })).toHaveAttribute(
      "href",
      "/dags/deadline_dag/runs/scheduled_run/callbacks",
    );
    expect(screen.getByRole("link", { name: "asset_with_dependencies / my_watcher" })).toHaveAttribute(
      "href",
      "/assets/1",
    );
    expect(screen.getByRole("link", { name: "3" })).toHaveAttribute("href", "/triggers/3");

    expect(screen.getAllByTestId("trigger-status-badge").map((badge) => badge.textContent)).toEqual([
      "states.running",
      "states.pending",
      "triggers.status.failing",
    ]);
    expect(screen.getByText("MessageQueueTrigger")).toBeInTheDocument();
    expect(screen.getByTitle("ConnectionError: broker.example.com:9092 is unreachable")).toBeInTheDocument();
  });

  it("counts each Dag a watcher's trigger runs once and lists them, and no Dags for other kinds", async () => {
    const shared = alsoWatching({ scheduled_dag_ids: ["consumer_dag", "report_dag"] });

    server.use(
      http.get("/api/v2/triggers", () =>
        HttpResponse.json({ total_entries: 2, triggers: [triggers[0], shared] }),
      ),
    );
    renderAt("/triggers");

    const watcherRow = await screen.findByRole("row", { name: /^3\b/u }, { timeout: 5000 });
    const taskRow = screen.getByRole("row", { name: /^1\b/u });
    const column = screen
      .getAllByRole("columnheader")
      .findIndex((header) => header.textContent === "triggers.columns.dagsAffected");

    const dagsAffected = within(within(watcherRow).getAllByRole("cell")[column] as HTMLElement).getByRole(
      "button",
    );

    expect(dagsAffected).toHaveTextContent(/^2 dag$/u);
    expect(within(taskRow).getAllByRole("cell")[column]).toHaveTextContent(/^-$/u);

    fireEvent.click(dagsAffected);

    expect(await screen.findByRole("link", { name: "consumer_dag" })).toHaveAttribute(
      "href",
      "/dags/consumer_dag",
    );
    expect(screen.getByRole("link", { name: "report_dag" })).toHaveAttribute("href", "/dags/report_dag");
  });

  it("says how long ago a trigger last failed under its last error", async () => {
    vi.useFakeTimers({ now: new Date("2026-10-01T03:04:00Z"), toFake: ["Date"] });
    renderAt("/triggers");

    expect(await screen.findByTestId("trigger-last-failed", undefined, { timeout: 5000 })).toHaveTextContent(
      "3 hours ago",
    );
    vi.useRealTimers();
  });

  it("sends the kind and status filters in the URL to the API", async () => {
    const requests: Array<{ kind: Array<string>; status: Array<string> }> = [];

    // Answered by the default handler, which filters on both.
    server.use(
      http.get("/api/v2/triggers", ({ request }) => {
        const params = new URL(request.url).searchParams;

        requests.push({ kind: params.getAll("kind"), status: params.getAll("status") });
      }),
    );

    renderAt("/triggers?kind=asset_watcher&status=failing");

    expect(
      await screen.findByRole("link", { name: "asset_with_dependencies / my_watcher" }),
    ).toBeInTheDocument();
    expect(requests.at(-1)).toEqual({ kind: ["asset_watcher"], status: ["failing"] });
    expect(screen.queryByRole("link", { name: "deferring_dag / wait" })).not.toBeInTheDocument();
  });

  it("explains a 403 from a user who can see nothing a trigger is used by", async () => {
    server.use(
      http.get("/api/v2/triggers", () => HttpResponse.json({ detail: "Forbidden" }, { status: 403 })),
    );

    renderAt("/triggers");

    expect(await screen.findByTestId("triggers-forbidden")).toHaveTextContent("triggers.forbidden.title");
  });
});
