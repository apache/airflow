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
import { render, screen, within } from "@testing-library/react";
import { http, HttpResponse } from "msw";
import { MemoryRouter } from "react-router-dom";
import { describe, expect, it } from "vitest";

import type { WebhookResponse } from "openapi/requests/types.gen";

import { TimezoneProvider } from "src/context/timezone";
import { server } from "src/mocks/server";
import { AppWrapper } from "src/utils/AppWrapper";
import { BaseWrapper } from "src/utils/Wrapper";

import { Webhooks } from "./Webhooks";

const webhooksUrl = "/api/v2/webhooks";

const buildWebhookRow = (overrides: Partial<WebhookResponse>): WebhookResponse => ({
  connection_status: "ok",
  last_outcome: null,
  last_received_at: null,
  listed: true,
  name: "demo",
  source_type: "hmac_json",
  source_type_status: "ok",
  ...overrides,
});

const serveWebhooks = (webhooks: Array<WebhookResponse>) =>
  server.use(http.get(webhooksUrl, () => HttpResponse.json({ total_entries: webhooks.length, webhooks })));

describe("Webhooks", () => {
  it("lists each webhook with what keeps it from being served and links it to its deliveries", async () => {
    serveWebhooks([
      buildWebhookRow({
        last_outcome: "accepted",
        last_received_at: "2026-10-18T12:00:00Z",
        name: "github",
        source_type: "github",
      }),
      buildWebhookRow({
        connection_status: "not_found",
        last_outcome: "dropped",
        last_received_at: "2026-10-17T12:00:00Z",
        listed: false,
        name: "gitlab",
        source_type: null,
        source_type_status: null,
      }),
      buildWebhookRow({ connection_status: "misconfigured", name: "jira", source_type_status: "conflict" }),
    ]);

    render(<AppWrapper initialEntries={["/webhook_overview"]} />);

    const github = await screen.findByRole("row", { name: /github/u });
    const gitlab = screen.getByRole("row", { name: /gitlab/u });
    const jira = screen.getByRole("row", { name: /jira/u });

    expect(screen.getByRole("heading", { name: "webhooks.title" })).toBeInTheDocument();
    expect(within(github).getByRole("link", { name: "github" })).toHaveAttribute(
      "href",
      "/webhook_deliveries/github",
    );
    expect(within(github).getByText("yes")).toBeInTheDocument();
    expect(within(github).getByText("webhooks.connectionStatuses.ok")).toBeInTheDocument();
    expect(within(github).getByTestId("time-display")).toHaveAttribute("dateTime", "2026-10-18T12:00:00Z");
    expect(within(github).getByText("webhookDeliveries.outcomes.accepted")).toBeInTheDocument();
    // Nothing keeps github from being served, so nothing is explained.
    expect(within(github).queryByText(/Explanations|notListed/u)).not.toBeInTheDocument();

    expect(within(gitlab).getByText("no")).toBeInTheDocument();
    expect(within(gitlab).getByText("webhooks.notListed")).toBeInTheDocument();
    expect(within(gitlab).getByText("webhooks.connectionStatuses.not_found")).toBeInTheDocument();
    expect(within(gitlab).getByText("webhooks.connectionStatusExplanations.not_found")).toBeInTheDocument();
    expect(within(gitlab).getByText("dropped")).toBeInTheDocument();

    expect(within(jira).getByText("webhooks.connectionStatuses.misconfigured")).toBeInTheDocument();
    expect(within(jira).getByText("webhooks.connectionStatusExplanations.misconfigured")).toBeInTheDocument();
    expect(within(jira).getByText("hmac_json")).toBeInTheDocument();
    expect(within(jira).getByText("webhooks.sourceTypeStatuses.conflict")).toBeInTheDocument();
    expect(within(jira).getByText("webhooks.sourceTypeStatusExplanations.conflict")).toBeInTheDocument();
  });

  it("is a tab of the webhook deliveries", async () => {
    serveWebhooks([]);

    render(<AppWrapper initialEntries={["/webhook_overview"]} />);

    expect(await screen.findByRole("link", { name: "webhookDeliveries.tabs.deliveries" })).toHaveAttribute(
      "href",
      "/webhook_deliveries",
    );
    expect(
      within(screen.getByRole("link", { name: "webhookDeliveries.tabs.webhooks" })).getByText(
        "webhookDeliveries.tabs.webhooks",
      ),
    ).toHaveAttribute("aria-current", "page");
  });

  it("shows an error from the API", async () => {
    const detail = "The [webhooks] configuration is invalid";

    server.use(http.get(webhooksUrl, () => HttpResponse.json({ detail }, { status: 500 })));

    // Without the app's retries, so the error shows at once.
    render(
      <QueryClientProvider client={new QueryClient({ defaultOptions: { queries: { retry: false } } })}>
        <MemoryRouter initialEntries={["/webhook_overview"]}>
          <TimezoneProvider>
            <Webhooks />
          </TimezoneProvider>
        </MemoryRouter>
      </QueryClientProvider>,
      { wrapper: BaseWrapper },
    );

    const alert = await screen.findByTestId("error-alert");

    expect(alert).toHaveTextContent("500");
    expect(alert).toHaveTextContent(detail);
  });
});
