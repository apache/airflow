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
import { MemoryRouter, Route, Routes } from "react-router-dom";
import { afterEach, describe, expect, it, onTestFinished, vi } from "vitest";

import type { WebhookDeliveryResponse, WebhookResponse } from "openapi/requests/types.gen";

import { TimezoneProvider } from "src/context/timezone";
import { server } from "src/mocks/server";
import { AppWrapper } from "src/utils/AppWrapper";
import { BaseWrapper } from "src/utils/Wrapper";

import { WebhookDeliveries } from "./WebhookDeliveries";

vi.mock("src/components/MonacoEditor", () => ({
  default: ({ value }: { readonly value?: string }) => <div data-testid="monaco-editor">{value}</div>,
}));

const deliveriesUrl = "/api/v2/webhooks/:webhookName/deliveries";
const deliveryRecordId = "0199f8a0-0000-7000-8000-000000000002";

const buildDeliveryRow = (overrides: Partial<WebhookDeliveryResponse>): WebhookDeliveryResponse => ({
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

const detailedDelivery = buildDeliveryRow({
  actions: {
    asset_events: [
      { asset: "demo/pull_request", asset_id: 3, event_id: 7 },
      { asset: "demo/pull_request/labeled", asset_id: 4, event_id: 8 },
    ],
    other_action: { count: 1 },
  },
  attributes: { action: "labeled", event: "pull_request", number: 7 },
  candidates: ["demo/pull_request", "demo/pull_request/labeled"],
  correlation_key: "octo/repo#7",
  delivery_id: "gh-2",
  id: deliveryRecordId,
  outcome: "accepted",
  source_type: "github",
});

/** Serve `rows` for the list and `detail` for a request naming a delivery id, recording each URL. */
const serveDeliveries = (
  rows: Array<WebhookDeliveryResponse>,
  detail: Array<WebhookDeliveryResponse> = [detailedDelivery],
) => {
  const requests: Array<URL> = [];

  server.use(
    http.get(deliveriesUrl, ({ request }) => {
      const url = new URL(request.url);

      requests.push(url);

      const deliveries = url.searchParams.has("id") ? detail : rows;

      return HttpResponse.json({ deliveries, total_entries: deliveries.length });
    }),
  );

  return requests;
};

const webhooksUrl = "/api/v2/webhooks";

const readableWebhooks: Array<WebhookResponse> = ["demo", "other"].map((name) => ({
  connection_status: "ok",
  last_outcome: null,
  last_received_at: null,
  listed: true,
  name,
  source_type: "hmac_json",
  source_type_status: "ok",
}));

const readableWebhooksResponse = () =>
  HttpResponse.json({ total_entries: readableWebhooks.length, webhooks: readableWebhooks });

const filterListRequests = (requests: Array<URL>) => requests.filter((url) => !url.searchParams.has("id"));
const filterDetailRequests = (requests: Array<URL>) => requests.filter((url) => url.searchParams.has("id"));

/** Answer with `response` only once `signal` aborts, so a test sees the page while it waits. */
const holdResponseUntil = (signal: AbortSignal, response: Response) =>
  new Promise<Response>((resolve) => {
    signal.addEventListener("abort", () => resolve(response));
  });

const apiErrors = [
  { detail: "Access to the webhook `nope` is denied", status: 403 },
  { detail: "The webhook `nope` was not found", status: 404 },
];

/** Render the page without the app's retries, so an error response shows at once. */
const renderWithoutRetries = (initialEntry: string) => {
  const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } });

  render(
    <QueryClientProvider client={queryClient}>
      <MemoryRouter initialEntries={[initialEntry]}>
        <TimezoneProvider>
          <Routes>
            <Route element={<WebhookDeliveries />} path="webhook_deliveries" />
            <Route element={<WebhookDeliveries />} path="webhook_deliveries/:webhookName" />
          </Routes>
        </TimezoneProvider>
      </MemoryRouter>
    </QueryClientProvider>,
    { wrapper: BaseWrapper },
  );
};

afterEach(() => {
  // The table keeps the last sort per page path.
  localStorage.clear();
});

describe("WebhookDeliveries", () => {
  it("lists every readable webhook's deliveries and links each to its webhook's page", async () => {
    const requests = serveDeliveries([
      detailedDelivery,
      buildDeliveryRow({ delivery_id: "gh-1", webhook: "other" }),
      buildDeliveryRow({
        delivery_id: "gh-0",
        id: "0199f8a0-0000-7000-8000-000000000000",
        outcome: "dropped",
      }),
    ]);

    render(<AppWrapper initialEntries={["/webhook_deliveries"]} />);

    const accepted = await screen.findByRole("row", { name: /gh-2/u });
    const otherWebhook = screen.getByRole("row", { name: /gh-1/u });
    const unknownOutcome = screen.getByRole("row", { name: /gh-0/u });

    expect(screen.getByRole("heading", { name: "browse.webhookDeliveries" })).toBeInTheDocument();
    expect(within(accepted).getByRole("link", { name: "demo" })).toHaveAttribute(
      "href",
      "/webhook_deliveries/demo",
    );
    expect(within(otherWebhook).getByRole("link", { name: "other" })).toHaveAttribute(
      "href",
      "/webhook_deliveries/other",
    );
    expect(within(accepted).getByText("webhookDeliveries.outcomes.accepted")).toBeInTheDocument();
    expect(within(accepted).getByText("octo/repo#7")).toBeInTheDocument();
    expect(within(accepted).getByText("github")).toBeInTheDocument();
    // Attributes, topic names and actions are only in the details panel.
    expect(within(accepted).queryByText("labeled")).not.toBeInTheDocument();
    expect(within(accepted).queryByText("demo/pull_request")).not.toBeInTheDocument();
    expect(within(otherWebhook).getByText("webhookDeliveries.outcomes.no_subscribers")).toBeInTheDocument();
    expect(within(unknownOutcome).getByText("dropped")).toBeInTheDocument();
    expect(new Set(filterListRequests(requests).map((url) => url.pathname))).toEqual(
      new Set(["/api/v2/webhooks/~/deliveries"]),
    );
  });

  it("presets the webhook named in the route", async () => {
    const requests = serveDeliveries([buildDeliveryRow({ delivery_id: "gh-1" })]);

    // The webhook filter belongs to the all-webhooks view, so a stale one in the URL is not sent.
    render(<AppWrapper initialEntries={["/webhook_deliveries/demo?webhook_name=other"]} />);

    await screen.findByRole("row", { name: /gh-1/u });

    expect(screen.getByRole("heading", { name: "webhookDeliveries.title" })).toBeInTheDocument();
    expect(screen.queryByText("webhookDeliveries.columns.webhook")).not.toBeInTheDocument();
    expect(screen.queryByTestId("webhook_name-pill")).not.toBeInTheDocument();
    expect(new Set(filterListRequests(requests).map((url) => url.pathname))).toEqual(
      new Set(["/api/v2/webhooks/demo/deliveries"]),
    );
    expect(filterListRequests(requests).every((url) => !url.searchParams.has("webhook_names"))).toBe(true);
  });

  it("sends the filters in the URL and shows them as removable filters", async () => {
    const requests = serveDeliveries([buildDeliveryRow({ delivery_id: "gh-2" })]);
    const filters = {
      correlation_key: "octo/repo#7",
      delivery_id: "gh-2",
      outcome: "accepted",
      received_at_gte: "2026-10-18T00:00:00Z",
      received_at_lte: "2026-10-19T00:00:00Z",
      source_type: "github",
    };

    render(
      <AppWrapper
        initialEntries={[
          `/webhook_deliveries?${new URLSearchParams({ ...filters, webhook_name: "demo" }).toString()}`,
        ]}
      />,
    );

    await waitFor(() => expect(filterListRequests(requests)).not.toHaveLength(0));
    const [request] = filterListRequests(requests);

    for (const [param, value] of Object.entries(filters)) {
      expect(request?.searchParams.get(param)).toBe(value);
    }
    expect(request?.searchParams.getAll("webhook_names")).toEqual(["demo"]);
    expect(await screen.findByTestId("webhook_name-pill")).toHaveTextContent("demo");
    expect(screen.getByTestId("source_type-pill")).toHaveTextContent("github");
  });

  it("is the Deliveries tab next to the Webhooks tab", async () => {
    serveDeliveries([]);

    render(<AppWrapper initialEntries={["/webhook_deliveries"]} />);

    expect(await screen.findByRole("link", { name: "webhookDeliveries.tabs.webhooks" })).toHaveAttribute(
      "href",
      "/webhook_overview",
    );
    expect(
      within(screen.getByRole("link", { name: "webhookDeliveries.tabs.deliveries" })).getByText(
        "webhookDeliveries.tabs.deliveries",
      ),
    ).toHaveAttribute("aria-current", "page");
  });

  it("offers the webhooks the user may read in the webhook filter, asking for them only once it is shown", async () => {
    const requests = serveDeliveries([buildDeliveryRow({ delivery_id: "gh-1" })]);
    const webhooksRequests: Array<URL> = [];

    server.use(
      http.get(webhooksUrl, ({ request }) => {
        webhooksRequests.push(new URL(request.url));

        return readableWebhooksResponse();
      }),
    );

    render(<AppWrapper initialEntries={["/webhook_deliveries"]} />);

    await screen.findByRole("row", { name: /gh-1/u });
    expect(webhooksRequests).toHaveLength(0);

    fireEvent.click(screen.getByTestId("add-filter-button"));
    fireEvent.click(await screen.findByTestId("add-filter-webhook_name"));

    expect(await screen.findByRole("option", { name: "demo" })).toBeInTheDocument();
    expect(webhooksRequests).not.toHaveLength(0);
    fireEvent.click(screen.getByRole("option", { name: "other" }));

    await waitFor(() =>
      expect(filterListRequests(requests).at(-1)?.searchParams.getAll("webhook_names")).toEqual(["other"]),
    );
  });

  it("says the webhook filter's webhooks are loading", async () => {
    const release = new AbortController();

    serveDeliveries([]);
    server.use(http.get(webhooksUrl, () => holdResponseUntil(release.signal, readableWebhooksResponse())));

    render(<AppWrapper initialEntries={["/webhook_deliveries"]} />);

    fireEvent.click(await screen.findByTestId("add-filter-button"));
    fireEvent.click(await screen.findByTestId("add-filter-webhook_name"));

    expect(await screen.findByText("loading")).toBeInTheDocument();
    expect(screen.queryByText("table.noResultsFound")).not.toBeInTheDocument();

    release.abort();

    expect(await screen.findByRole("option", { name: "demo" })).toBeInTheDocument();
  });

  it("says when the webhook filter cannot load the webhooks, and filters by a typed name", async () => {
    const requests = serveDeliveries([]);

    server.use(
      http.get(webhooksUrl, () =>
        HttpResponse.json({ detail: "The [webhooks] configuration is invalid" }, { status: 500 }),
      ),
    );

    renderWithoutRetries("/webhook_deliveries");

    fireEvent.click(await screen.findByTestId("add-filter-button"));
    fireEvent.click(await screen.findByTestId("add-filter-webhook_name"));

    expect(await screen.findByText("webhooks.filterLoadError")).toBeInTheDocument();
    expect(screen.queryByText("table.noResultsFound")).not.toBeInTheDocument();

    const input = screen.getByRole("combobox", { name: "webhookDeliveries.columns.webhook" });

    fireEvent.change(input, { target: { value: "demo" } });
    fireEvent.keyDown(input, { key: "Enter" });

    await waitFor(() =>
      expect(filterListRequests(requests).at(-1)?.searchParams.getAll("webhook_names")).toEqual(["demo"]),
    );
  });

  it("sorts by delivery ID through the API", async () => {
    const requests = serveDeliveries([buildDeliveryRow({ delivery_id: "gh-1" })]);

    render(<AppWrapper initialEntries={["/webhook_deliveries/demo"]} />);

    fireEvent.click(
      (await screen.findByText("webhookDeliveries.columns.deliveryId")).closest(
        "button",
      ) as HTMLButtonElement,
    );

    await waitFor(() =>
      expect(filterListRequests(requests).at(-1)?.searchParams.getAll("order_by")).toEqual(["delivery_id"]),
    );
  });

  it("opens a delivery's details from its row and closes them again", async () => {
    const requests = serveDeliveries([detailedDelivery]);

    render(<AppWrapper initialEntries={["/webhook_deliveries"]} />);

    const row = await screen.findByRole("row", { name: /gh-2/u });

    fireEvent.click(within(row).getByRole("button", { name: "webhookDeliveries.details.show" }));

    const panel = await screen.findByTestId("delivery-details");

    await waitFor(() => expect(within(panel).getByText("github")).toBeInTheDocument());
    expect(filterDetailRequests(requests).at(-1)?.searchParams.get("id")).toBe(deliveryRecordId);
    expect(filterDetailRequests(requests).at(-1)?.pathname).toBe("/api/v2/webhooks/~/deliveries");
    expect(within(panel).getByText(deliveryRecordId)).toBeInTheDocument();
    expect(within(panel).getByText("octo/repo#7")).toBeInTheDocument();
    expect(within(panel).getByRole("link", { name: "demo" })).toHaveAttribute(
      "href",
      "/webhook_deliveries/demo",
    );

    const attributes = within(panel).getByTestId("delivery-attributes");

    expect(within(attributes).getByRole("row", { name: "action labeled" })).toBeInTheDocument();
    expect(within(attributes).getByRole("row", { name: "number 7" })).toBeInTheDocument();

    fireEvent.click(within(panel).getByRole("button", { name: "Close" }));

    await waitFor(() => expect(screen.queryByTestId("delivery-details")).not.toBeInTheDocument());
  });

  it("copies each topic name, links each asset event to its asset, and shows other actions as JSON", async () => {
    serveDeliveries([], [detailedDelivery]);
    const writeText = vi.spyOn(navigator.clipboard, "writeText").mockResolvedValue();

    render(<AppWrapper initialEntries={[`/webhook_deliveries?id=${deliveryRecordId}`]} />);

    const panel = await screen.findByTestId("delivery-details");
    const assetEvents = await within(panel).findByTestId("delivery-asset-events");

    expect(
      within(assetEvents)
        .getAllByRole("listitem")
        .map((item) => within(item).getByRole("link"))
        .map((link) => [link.textContent, link.getAttribute("href")]),
    ).toEqual([
      ["demo/pull_request", "/assets/3"],
      ["demo/pull_request/labeled", "/assets/4"],
    ]);
    expect(within(panel).getByTestId("monaco-editor")).toHaveTextContent(
      JSON.stringify({ other_action: { count: 1 } }, undefined, 2),
      { normalizeWhitespace: false },
    );

    const candidate = within(panel).getByTestId("delivery-candidates");
    const [, copyLabeled] = within(candidate).getAllByRole("button");

    fireEvent.click(copyLabeled as HTMLElement);

    await waitFor(() => expect(writeText).toHaveBeenCalledWith("demo/pull_request/labeled"));
  });

  it.each([
    { assetEvents: { "demo/push": 8 }, shape: "a map of names to event ids" },
    { assetEvents: [{ asset: "demo/push", event_id: 8 }], shape: "a list without asset ids" },
  ])("shows asset events recorded as $shape as recorded", async ({ assetEvents }) => {
    serveDeliveries([], [buildDeliveryRow({ actions: { asset_events: assetEvents }, id: deliveryRecordId })]);

    render(<AppWrapper initialEntries={[`/webhook_deliveries?id=${deliveryRecordId}`]} />);

    const panel = await screen.findByTestId("delivery-details");

    expect(await within(panel).findByTestId("monaco-editor")).toHaveTextContent(
      JSON.stringify({ asset_events: assetEvents }, undefined, 2),
      { normalizeWhitespace: false },
    );
    expect(within(panel).queryByTestId("delivery-asset-events")).not.toBeInTheDocument();
  });

  it("links each asset route event to its asset, and lists each skipped route with its reason", async () => {
    serveDeliveries(
      [],
      [
        buildDeliveryRow({
          actions: {
            asset_routes: [
              {
                asset: "s3://warehouse/orders",
                asset_id: 5,
                event_id: 9,
                partition_key: "2026-10-19",
                topic: "demo/orders/load.completed",
              },
              {
                asset: "invoices",
                asset_id: 6,
                event_id: 10,
                partition_key: null,
                topic: "demo/*/load.completed",
              },
            ],
            skipped_asset_routes: [
              {
                asset: "s3://warehouse/refunds",
                partition_key_field: null,
                reason: "asset_not_active",
                topic: "demo/*/load.completed",
              },
              {
                asset: "s3://warehouse/returns",
                partition_key_field: null,
                reason: "renamed_later",
                topic: "demo/*/load.completed",
              },
            ],
          },
          id: deliveryRecordId,
          outcome: "accepted",
        }),
      ],
    );

    render(<AppWrapper initialEntries={[`/webhook_deliveries?id=${deliveryRecordId}`]} />);

    const panel = await screen.findByTestId("delivery-details");
    const routeEvents = await within(panel).findByTestId("delivery-asset-route-events");
    const [keyed, unkeyed] = within(routeEvents).getAllByRole("listitem") as [HTMLElement, HTMLElement];

    expect(
      within(routeEvents)
        .getAllByRole("link")
        .map((link) => [link.textContent, link.getAttribute("href")]),
    ).toEqual([
      ["s3://warehouse/orders", "/assets/5"],
      ["invoices", "/assets/6"],
    ]);
    expect(within(keyed).getByText("demo/orders/load.completed")).toBeInTheDocument();
    expect(within(keyed).getByText("2026-10-19")).toBeInTheDocument();
    expect(within(unkeyed).getByText("demo/*/load.completed")).toBeInTheDocument();
    expect(within(unkeyed).queryByText("dagRun.partitionKey")).not.toBeInTheDocument();

    const skippedRoutes = within(panel).getByTestId("delivery-skipped-asset-routes");
    const [notActive, unknownReason] = within(skippedRoutes).getAllByRole("listitem") as [
      HTMLElement,
      HTMLElement,
    ];

    expect(within(skippedRoutes).queryByRole("link")).not.toBeInTheDocument();
    expect(within(notActive).getByText("s3://warehouse/refunds")).toBeInTheDocument();
    expect(within(notActive).getByText("webhookDeliveries.skipReasons.asset_not_active")).toBeInTheDocument();
    expect(within(unknownReason).getByText("webhookDeliveries.skipReasons.unknown")).toBeInTheDocument();
    expect(within(panel).queryByTestId("monaco-editor")).not.toBeInTheDocument();
  });

  it("lists each skipped route with its partition key field, even routes that are alike", async () => {
    const consoleError = vi.spyOn(console, "error");

    onTestFinished(() => consoleError.mockRestore());
    const route = { asset: "invoices", partition_key_field: null, reason: "dag_paused", topic: "demo/x" };
    const actions = { skipped_asset_routes: [{ ...route, partition_key_field: "data.date" }, route, route] };

    serveDeliveries([], [buildDeliveryRow({ actions, id: deliveryRecordId })]);
    render(<AppWrapper initialEntries={[`/webhook_deliveries?id=${deliveryRecordId}`]} />);

    const skippedRoutes = await screen.findByTestId("delivery-skipped-asset-routes");
    const [keyed, ...alike] = within(skippedRoutes).getAllByRole("listitem");
    const fieldLabel = "webhookDeliveries.details.partitionKeyField";

    expect(within(keyed as HTMLElement).getByText("data.date")).toBeInTheDocument();
    expect(alike.map((item) => within(item).queryByText(fieldLabel))).toEqual([null, null]);
    expect(consoleError.mock.calls.flat().join(" ")).not.toContain("same key");
  });

  it.each([
    {
      actions: { asset_routes: [{ asset: "invoices", asset_id: 6, event_id: 10 }] },
      shape: "route events without a topic",
    },
    { actions: { skipped_asset_routes: ["invoices"] }, shape: "skipped routes that are not objects" },
    {
      actions: {
        skipped_asset_routes: [{ asset: "invoices", reason: "dag_paused", topic: "demo/invoices" }],
      },
      shape: "skipped routes without a partition key field",
    },
  ])("shows $shape as recorded", async ({ actions }) => {
    serveDeliveries([], [buildDeliveryRow({ actions, id: deliveryRecordId })]);

    render(<AppWrapper initialEntries={[`/webhook_deliveries?id=${deliveryRecordId}`]} />);

    const panel = await screen.findByTestId("delivery-details");

    expect(await within(panel).findByTestId("monaco-editor")).toHaveTextContent(
      JSON.stringify(actions, undefined, 2),
      { normalizeWhitespace: false },
    );
    expect(within(panel).queryByTestId("delivery-asset-route-events")).not.toBeInTheDocument();
    expect(within(panel).queryByTestId("delivery-skipped-asset-routes")).not.toBeInTheDocument();
  });

  it.each([
    { explanation: "webhookDeliveries.outcomeExplanations.accepted", outcome: "accepted" },
    { explanation: "webhookDeliveries.outcomeExplanations.dag_paused", outcome: "dag_paused" },
    { explanation: "webhookDeliveries.outcomeExplanations.no_subscribers", outcome: "no_subscribers" },
    { explanation: "webhookDeliveries.outcomeExplanations.unknown", outcome: "dropped" },
  ])("explains the $outcome outcome", async ({ explanation, outcome }) => {
    serveDeliveries([], [buildDeliveryRow({ id: deliveryRecordId, outcome })]);

    render(<AppWrapper initialEntries={[`/webhook_deliveries/demo?id=${deliveryRecordId}`]} />);

    const panel = await screen.findByTestId("delivery-details");

    expect(await within(panel).findByText(explanation)).toBeInTheDocument();
  });

  it("opens the delivery a link names, whichever page the table shows", async () => {
    const requests = serveDeliveries([buildDeliveryRow({ delivery_id: "gh-1" })]);

    render(<AppWrapper initialEntries={[`/webhook_deliveries/demo?id=${deliveryRecordId}`]} />);

    const panel = await screen.findByTestId("delivery-details");

    await waitFor(() => expect(within(panel).getByText("gh-2")).toBeInTheDocument());
    expect(filterDetailRequests(requests).at(-1)?.pathname).toBe("/api/v2/webhooks/demo/deliveries");
    expect(filterDetailRequests(requests).at(-1)?.searchParams.get("limit")).toBe("1");
    // The id opens the panel; it does not filter the table.
    expect(await screen.findByRole("row", { name: /gh-1/u })).toBeInTheDocument();
  });

  it("says when a linked delivery is not one the user may read", async () => {
    serveDeliveries([], []);

    render(<AppWrapper initialEntries={[`/webhook_deliveries?id=${deliveryRecordId}`]} />);

    const panel = await screen.findByTestId("delivery-details");

    expect(await within(panel).findByText("webhookDeliveries.details.notFound")).toBeInTheDocument();
  });

  it("shows a spinner while a linked delivery loads", async () => {
    const release = new AbortController();

    server.use(
      http.get(deliveriesUrl, ({ request }) =>
        new URL(request.url).searchParams.has("id")
          ? holdResponseUntil(
              release.signal,
              HttpResponse.json({ deliveries: [detailedDelivery], total_entries: 1 }),
            )
          : HttpResponse.json({ deliveries: [], total_entries: 0 }),
      ),
    );

    render(<AppWrapper initialEntries={[`/webhook_deliveries?id=${deliveryRecordId}`]} />);

    const panel = await screen.findByTestId("delivery-details");

    expect(await within(panel).findByTestId("delivery-details-loading")).toBeInTheDocument();

    release.abort();

    expect(await within(panel).findByText("gh-2")).toBeInTheDocument();
    expect(within(panel).queryByTestId("delivery-details-loading")).not.toBeInTheDocument();
  });

  it.each(apiErrors)("shows a $status from the API", async ({ detail, status }) => {
    server.use(http.get(deliveriesUrl, () => HttpResponse.json({ detail }, { status })));

    renderWithoutRetries("/webhook_deliveries/nope");

    const alert = await screen.findByTestId("error-alert");

    expect(alert).toHaveTextContent(String(status));
    expect(alert).toHaveTextContent(detail);
  });

  it.each(apiErrors)("shows a $status for a linked delivery in its panel", async ({ detail, status }) => {
    server.use(
      http.get(deliveriesUrl, ({ request }) =>
        new URL(request.url).searchParams.has("id")
          ? HttpResponse.json({ detail }, { status })
          : HttpResponse.json({ deliveries: [], total_entries: 0 }),
      ),
    );

    renderWithoutRetries(`/webhook_deliveries/nope?id=${deliveryRecordId}`);

    const panel = await screen.findByTestId("delivery-details");
    const alert = await within(panel).findByTestId("error-alert");

    expect(alert).toHaveTextContent(String(status));
    expect(alert).toHaveTextContent(detail);
    expect(within(panel).queryByText("webhookDeliveries.details.notFound")).not.toBeInTheDocument();
  });
});
