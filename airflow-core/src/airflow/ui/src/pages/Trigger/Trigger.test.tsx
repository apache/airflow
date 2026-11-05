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
import { render, screen, within } from "@testing-library/react";
import { http, HttpResponse } from "msw";
import { createMemoryRouter, RouterProvider } from "react-router-dom";
import { beforeAll, describe, expect, it } from "vitest";

import { TimezoneProvider } from "src/context/timezone";
import { triggers } from "src/mocks/handlers/triggers";
import { server } from "src/mocks/server";
import { routerConfig } from "src/router";
import { preloadAppRoutes } from "src/utils/AppWrapper";

beforeAll(() => preloadAppRoutes(["/triggers/1", "/triggers/1/owner"]));

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

// Tests render i18n keys: the msw handlers serve empty translation bundles.
describe("Trigger details", () => {
  it("shows why a failing trigger fails on its Details tab", async () => {
    renderAt("/triggers/3");

    expect(await screen.findByTestId("trigger-last-error")).toHaveTextContent(
      "ConnectionError: broker.example.com:9092 is unreachable",
    );
    expect(screen.getAllByText("triggers.status.failing").length).toBeGreaterThan(0);
    expect(screen.getByRole("row", { name: /triggers\.columns\.failures\s*3/u })).toBeInTheDocument();
    expect(screen.getByRole("row", { name: /triggers\.detail\.firstFailedAt/u })).toHaveTextContent(
      "2026-10-01",
    );
  });

  it("names the trigger by its class in the header", async () => {
    renderAt("/triggers/3");

    expect(await screen.findByRole("heading", { name: "MessageQueueTrigger" })).toBeInTheDocument();
    expect(screen.getByRole("link", { name: /tabs\.details/u })).toHaveAttribute("href", "/triggers/3");
  });

  it("leads back to the Triggers list from a breadcrumb", async () => {
    renderAt("/triggers/3/owner");

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
    const failing = triggers.find(({ id }) => id === 3);

    server.use(
      http.get("/api/v2/triggers/:triggerId", () =>
        HttpResponse.json({ ...failing, consecutive_failures: 0, first_failed_at: null, status: "running" }),
      ),
    );
    renderAt("/triggers/3");

    expect(await screen.findByTestId("trigger-last-error")).toHaveTextContent("ConnectionError");
    expect(screen.getAllByText("states.running").length).toBeGreaterThan(0);
    expect(screen.getByRole("row", { name: /triggers\.detail\.lastFailedAt/u })).toHaveTextContent(
      "2026-10-01",
    );
    expect(screen.queryByRole("row", { name: /triggers\.detail\.firstFailedAt/u })).not.toBeInTheDocument();
  });

  it("lists the Dags scheduled on a watcher's assets, once each, as the trigger lists them", async () => {
    const watcher = triggers.find(({ id }) => id === 3);
    const assetRequests: Array<string> = [];

    server.use(
      http.get("/api/v2/assets/:assetId", ({ request }) => {
        assetRequests.push(request.url);

        return HttpResponse.json({});
      }),
      http.get("/api/v2/triggers/:triggerId", () =>
        HttpResponse.json({
          ...watcher,
          asset_watchers: [
            ...(watcher?.asset_watchers ?? []),
            {
              asset_id: 2,
              asset_name: "plain_asset",
              asset_uri: "s3://bucket/plain",
              scheduled_dag_ids: ["audit_payments", "consumer_dag", "process_payments"],
              watcher_name: "my_watcher",
            },
          ],
        }),
      ),
    );
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
    const watcher = triggers.find(({ id }) => id === 3);
    const unreadable = watcher?.asset_watchers.map((entry) => ({ ...entry, scheduled_dag_ids: [] }));

    server.use(
      http.get("/api/v2/triggers/:triggerId", () =>
        HttpResponse.json({ ...watcher, asset_watchers: unreadable }),
      ),
    );
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

  it("shows a deferred task's try and state on the Owner tab", async () => {
    renderAt("/triggers/1/owner");

    expect(await screen.findByRole("row", { name: /tryNumber\s*1/u })).toBeInTheDocument();
    expect(screen.getByRole("row", { name: /^state/u })).toHaveTextContent("states.deferred");
  });

  it("shows a callback's state as a translated badge on the Owner tab", async () => {
    renderAt("/triggers/2/owner");

    expect(await screen.findByRole("row", { name: /^state/u })).toHaveTextContent("states.pending");
    expect(screen.getByTestId("state-badge")).toBeInTheDocument();
  });
});
