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
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import "@testing-library/jest-dom/vitest";
import { fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import { http, HttpResponse } from "msw";
import { setupServer, type SetupServer } from "msw/node";
import { MemoryRouter, Route, Routes } from "react-router-dom";
import { afterAll, afterEach, beforeAll, describe, expect, it } from "vitest";

import type { WebhookDeliveryResponse } from "openapi/requests/types.gen";

import { TimezoneProvider } from "src/context/timezone";
import { handlers } from "src/mocks/handlers";
import { AppWrapper } from "src/utils/AppWrapper";
import { BaseWrapper } from "src/utils/Wrapper";

import { WebhookDeliveries } from "./WebhookDeliveries";

let server: SetupServer;

const deliveriesUrl = "/api/v2/webhooks/:webhookName/deliveries";

const deliveryRow = (overrides: Partial<WebhookDeliveryResponse>): WebhookDeliveryResponse => ({
  actions: null,
  attributes: null,
  candidates: null,
  correlation_key: null,
  delivery_id: null,
  id: "0199f8a0-0000-7000-8000-000000000001",
  outcome: "no_subscribers",
  received_at: "2026-10-18T12:00:00Z",
  source_type: "hmac_json",
  team_name: null,
  webhook: "demo",
  ...overrides,
});

const captureRequests = () => {
  const requests: Array<URL> = [];

  server.use(
    http.get(deliveriesUrl, ({ request }) => {
      requests.push(new URL(request.url));

      return HttpResponse.json({ deliveries: [deliveryRow({ delivery_id: "gh-1" })], total_entries: 1 });
    }),
  );

  return requests;
};

beforeAll(() => {
  server = setupServer(...handlers);
  server.listen({ onUnhandledFrame: "bypass" });
});

afterEach(() => {
  server.resetHandlers();
  // The table keeps the last sort per page path.
  localStorage.clear();
});
afterAll(() => server.close());

describe("WebhookDeliveries", () => {
  it("lists the deliveries of the webhook named in the route", async () => {
    const requestedWebhooks = new Set<string>();

    server.use(
      http.get(deliveriesUrl, ({ params }) => {
        requestedWebhooks.add(String(params.webhookName));

        return HttpResponse.json({
          deliveries: [
            deliveryRow({
              actions: { asset_events: { "demo/pull_request/labeled": 7 } },
              attributes: { action: "labeled", event: "pull_request" },
              candidates: ["demo/pull_request", "demo/pull_request/labeled"],
              correlation_key: "pr-7",
              delivery_id: "gh-2",
              id: "0199f8a0-0000-7000-8000-000000000002",
              outcome: "accepted",
              source_type: "github",
            }),
            deliveryRow({ delivery_id: "gh-1" }),
            deliveryRow({
              actions: { asset_events: { "demo/push": 8 }, other_action: 1 },
              delivery_id: "gh-0",
              id: "0199f8a0-0000-7000-8000-000000000000",
              outcome: "dropped",
            }),
          ],
          total_entries: 3,
        });
      }),
    );

    render(<AppWrapper initialEntries={["/webhook_deliveries/demo"]} />);

    const accepted = await screen.findByRole("row", { name: /gh-2/u });
    const ignored = screen.getByRole("row", { name: /gh-1/u });
    const unknownOutcome = screen.getByRole("row", { name: /gh-0/u });

    expect(within(accepted).getByText("webhookDeliveries.outcomes.accepted")).toBeInTheDocument();
    expect(within(accepted).getByText("pr-7")).toBeInTheDocument();
    expect(within(accepted).getByText("github")).toBeInTheDocument();
    expect(within(accepted).getByText("labeled")).toBeInTheDocument();
    expect(within(accepted).getByText("demo/pull_request/labeled")).toBeInTheDocument();
    expect(within(accepted).getByText("demo/pull_request/labeled:")).toBeInTheDocument();
    expect(within(accepted).queryByText("asset_events:")).not.toBeInTheDocument();
    expect(within(ignored).getByText("webhookDeliveries.outcomes.no_subscribers")).toBeInTheDocument();
    expect(within(ignored).getByText("hmac_json")).toBeInTheDocument();
    expect(within(ignored).queryByTestId("json-preview-badges")).not.toBeInTheDocument();
    expect(within(unknownOutcome).getByText("dropped")).toBeInTheDocument();
    expect(within(unknownOutcome).getByText("asset_events:")).toBeInTheDocument();
    expect(within(unknownOutcome).getByText("other_action:")).toBeInTheDocument();
    expect(requestedWebhooks).toEqual(new Set(["demo"]));
  });

  it("sends the filters in the URL and shows them as removable filters", async () => {
    const requests = captureRequests();
    const deliveryRecordId = "0199f8a0-0000-7000-8000-000000000002";
    const filters = {
      correlation_key: "pr-7",
      delivery_id: "gh-2",
      id: deliveryRecordId,
      outcome: "accepted",
      received_at_gte: "2026-10-18T00:00:00Z",
      received_at_lte: "2026-10-19T00:00:00Z",
    };

    render(
      <AppWrapper initialEntries={[`/webhook_deliveries/demo?${new URLSearchParams(filters).toString()}`]} />,
    );

    await waitFor(() => expect(requests).not.toHaveLength(0));
    const [request] = requests;

    for (const [param, value] of Object.entries(filters)) {
      expect(request?.searchParams.get(param)).toBe(value);
    }
    expect(await screen.findByTestId("id-pill")).toHaveTextContent(deliveryRecordId);
  });

  it("sorts by delivery ID through the API", async () => {
    const requests = captureRequests();

    render(<AppWrapper initialEntries={["/webhook_deliveries/demo"]} />);

    fireEvent.click(
      (await screen.findByText("webhookDeliveries.columns.deliveryId")).closest(
        "button",
      ) as HTMLButtonElement,
    );

    await waitFor(() => expect(requests.at(-1)?.searchParams.getAll("order_by")).toEqual(["delivery_id"]));
  });

  it.each([
    { detail: "Access to the webhook `nope` is denied", status: 403 },
    { detail: "The webhook `nope` was not found", status: 404 },
  ])("shows a $status from the API", async ({ detail, status }) => {
    server.use(http.get(deliveriesUrl, () => HttpResponse.json({ detail }, { status })));

    const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } });

    render(
      <QueryClientProvider client={queryClient}>
        <MemoryRouter initialEntries={["/webhook_deliveries/nope"]}>
          <TimezoneProvider>
            <Routes>
              <Route element={<WebhookDeliveries />} path="webhook_deliveries/:webhookName" />
            </Routes>
          </TimezoneProvider>
        </MemoryRouter>
      </QueryClientProvider>,
      { wrapper: BaseWrapper },
    );

    const alert = await screen.findByTestId("error-alert");

    expect(alert).toHaveTextContent(String(status));
    expect(alert).toHaveTextContent(detail);
  });
});
