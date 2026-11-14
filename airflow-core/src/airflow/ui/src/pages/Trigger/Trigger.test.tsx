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
import { render, screen, waitFor, within } from "@testing-library/react";
import { http, HttpResponse } from "msw";
import { createMemoryRouter, RouterProvider } from "react-router-dom";
import { beforeAll, describe, expect, it } from "vitest";

import type { TriggerDetailResponse } from "openapi/requests/types.gen";

import { TimezoneProvider } from "src/context/timezone";
import { assetWithDependencies } from "src/mocks/handlers/assets";
import { alsoWatching, triggers } from "src/mocks/handlers/triggers";
import { server } from "src/mocks/server";
import { routerConfig } from "src/router";
import { preloadAppRoutes } from "src/utils/AppWrapper";

beforeAll(() => preloadAppRoutes(["/triggers/1", "/triggers/1/used_by"]));

const renderAt = (path: string) =>
  render(
    <ChakraProvider value={defaultSystem}>
      <QueryClientProvider client={new QueryClient({ defaultOptions: { queries: { retry: false } } })}>
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

const watcher = triggers.find(({ id }) => id === 3) as TriggerDetailResponse;

const watchedBy = (name: string) => [{ ...assetWithDependencies.watchers[0], name, trigger_id: 3 }];

const serveTrigger = (trigger: TriggerDetailResponse) =>
  server.use(http.get("/api/v2/triggers/:triggerId", () => HttpResponse.json(trigger)));

// Tests render i18n keys: the msw handlers serve empty translation bundles.
describe("Trigger details", () => {
  it("names a failing trigger by its class and shows why it fails on its Details tab", async () => {
    renderAt("/triggers/3");

    expect(await screen.findByTestId("trigger-last-error")).toHaveTextContent(
      "ConnectionError: broker.example.com:9092 is unreachable",
    );
    expect(screen.getByRole("heading", { name: "MessageQueueTrigger" })).toBeInTheDocument();
    expect(screen.getByRole("link", { name: /tabs\.details/u })).toHaveAttribute("href", "/triggers/3");
    expect(screen.getAllByText("triggers.status.failing").length).toBeGreaterThan(0);
    expect(screen.getByRole("row", { name: /triggers\.columns\.failures\s*3/u })).toBeInTheDocument();
    expect(screen.getByRole("row", { name: /triggers\.detail\.firstFailedAt/u })).toHaveTextContent(
      "2026-10-01",
    );
  });

  it("leads back to the Triggers list from a breadcrumb", async () => {
    renderAt("/triggers/3/used_by");

    const breadcrumb = await screen.findByTestId("trigger-breadcrumb");

    expect(within(breadcrumb).getByRole("link", { name: /browse\.triggers/u })).toHaveAttribute(
      "href",
      "/triggers",
    );
    expect(within(breadcrumb).getByText("3").closest("[aria-current='page']")).not.toBeNull();
  });

  it("leaves out the failure times of a trigger that is not failing", async () => {
    renderAt("/triggers/1");

    expect(await screen.findByRole("row", { name: /triggers\.columns\.failures\s*0/u })).toBeInTheDocument();
    expect(screen.queryByRole("row", { name: /triggers\.detail\.firstFailedAt/u })).not.toBeInTheDocument();
    expect(screen.queryByTestId("trigger-last-error")).not.toBeInTheDocument();
  });

  it("keeps the last error of a recovered trigger, muted, with when it last failed", async () => {
    serveTrigger({ ...watcher, consecutive_failures: 0, first_failed_at: null, status: "running" });
    renderAt("/triggers/3");

    expect(await screen.findByTestId("trigger-last-error")).toHaveTextContent("ConnectionError");
    expect(screen.getAllByText("states.running").length).toBeGreaterThan(0);
    expect(screen.getByRole("row", { name: /triggers\.detail\.lastFailedAt/u })).toHaveTextContent(
      "2026-10-01",
    );
    expect(screen.queryByRole("row", { name: /triggers\.detail\.firstFailedAt/u })).not.toBeInTheDocument();
  });

  it("lists the Dags scheduled on a watcher's assets, once each, as the trigger lists them", async () => {
    const assetRequests: Array<string> = [];

    server.use(
      http.get("/api/v2/assets/:assetId", ({ request }) => {
        assetRequests.push(request.url);

        return HttpResponse.json({});
      }),
    );
    serveTrigger(alsoWatching({ scheduled_dag_ids: ["audit_payments", "consumer_dag", "process_payments"] }));
    renderAt("/triggers/3");

    const row = await screen.findByRole("row", {
      name: /triggers\.detail\.scheduledDags.*process_payments/u,
    });

    expect(row).toHaveTextContent(/audit_paymentsconsumer_dagprocess_payments$/u);
    expect(screen.getByRole("link", { name: "process_payments" })).toHaveAttribute(
      "href",
      "/dags/process_payments",
    );
    expect(assetRequests).toEqual([]);
  });

  it("says when a watcher's assets have no Dag the user can read", async () => {
    serveTrigger({
      ...watcher,
      asset_watchers: watcher.asset_watchers.map((entry) => ({ ...entry, scheduled_dag_ids: [] })),
    });
    renderAt("/triggers/3");

    expect(
      await screen.findByRole("row", { name: /triggers\.detail\.noReadableScheduledDags/u }),
    ).toBeInTheDocument();
    expect(screen.getAllByRole("row", { name: /triggers\.detail\.scheduledDags/u })).toHaveLength(1);
  });

  it("explains that a trigger it cannot find has most likely finished", async () => {
    renderAt("/triggers/99");

    expect(await screen.findByText("triggers.notFound.title")).toBeInTheDocument();
    expect(screen.getByRole("link", { name: "triggers.notFound.backToList" })).toHaveAttribute(
      "href",
      "/triggers",
    );
  });

  it("shows a deferred task's try and state on the Used By tab", async () => {
    renderAt("/triggers/1/used_by");

    expect(await screen.findByRole("row", { name: /tryNumber\s*1/u })).toBeInTheDocument();
    expect(screen.getByRole("row", { name: /^state/u })).toHaveTextContent("states.deferred");
  });

  it("shows each watched asset's own latest event, from two requests that name only the trigger", async () => {
    const pageSize = 50;
    const recentEvents = 5;
    const assetRequests: Array<Record<string, string>> = [];
    const eventRequests: Array<Record<string, string>> = [];

    serveTrigger(alsoWatching({ asset_name: "quiet_asset", watcher_name: "quiet_watcher" }));
    server.use(
      http.get("/ui/config", () => HttpResponse.json({ fallback_page_limit: pageSize })),
      http.get("/api/v2/assets", ({ request }) => {
        assetRequests.push(Object.fromEntries(new URL(request.url).searchParams));

        return HttpResponse.json({
          assets: [
            { ...assetWithDependencies, watchers: watchedBy("my_watcher") },
            {
              ...assetWithDependencies,
              id: 2,
              last_asset_event: { id: 2, timestamp: "2026-09-01T12:00:00Z" },
              name: "quiet_asset",
              watchers: watchedBy("quiet_watcher"),
            },
          ],
          total_entries: 2,
        });
      }),
      http.get("/api/v2/assets/events", ({ request }) => {
        eventRequests.push(Object.fromEntries(new URL(request.url).searchParams));

        // None of them the quiet asset's, as when a busy asset's events fill the latest ones.
        return HttpResponse.json({ asset_events: [], total_entries: 0 });
      }),
    );
    renderAt("/triggers/3/used_by");

    const quietLink = await screen.findByRole("link", { name: "quiet_asset" });

    expect(quietLink).toHaveAttribute("href", "/assets/2");
    await waitFor(() =>
      expect(quietLink.closest("tr")).toHaveTextContent(/quiet_watcher.*2026-09-01 12:00:00/u),
    );
    // However many assets the trigger watches, neither URL grows with them.
    await waitFor(() =>
      expect(eventRequests).toEqual([
        { limit: String(recentEvents), order_by: "-timestamp", trigger_id: "3" },
      ]),
    );
    expect(assetRequests).toEqual([{ limit: String(pageSize), offset: "0", trigger_id: "3" }]);
  });

  it("shows a callback's state as a translated badge on the Used By tab", async () => {
    renderAt("/triggers/2/used_by");

    expect(await screen.findByRole("row", { name: /^state/u })).toHaveTextContent("states.pending");
    expect(screen.getByTestId("state-badge")).toBeInTheDocument();
  });
});
