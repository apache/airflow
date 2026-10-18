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
import { http, HttpResponse } from "msw";
import { setupServer, type SetupServer } from "msw/node";
import { afterAll, afterEach, beforeAll, describe, expect, it } from "vitest";

import type { ConnectionResponse } from "openapi/requests/types.gen";

import { handlers } from "src/mocks/handlers";
import { AppWrapper } from "src/utils/AppWrapper";

let server: SetupServer;

const connectionRow = (connectionId: string, connType: string): ConnectionResponse => ({
  conn_type: connType,
  connection_id: connectionId,
  description: null,
  extra: null,
  host: null,
  login: null,
  password: null,
  port: null,
  schema: null,
  team_name: null,
});

beforeAll(() => {
  server = setupServer(...handlers);
  server.listen({ onUnhandledFrame: "bypass" });
});

afterEach(() => server.resetHandlers());
afterAll(() => server.close());

describe("ViewWebhookDeliveriesButton", () => {
  it("links only the connection that serves a webhook to that webhook's deliveries", async () => {
    let requestedWebhook: string | undefined;

    server.use(
      http.get("/api/v2/connections", () => {
        const connections = [
          connectionRow("webhook_demo", "inbound_webhook"),
          connectionRow("webhook_http", "http"),
          connectionRow("unprefixed", "inbound_webhook"),
          connectionRow("webhook_", "inbound_webhook"),
          connectionRow("webhook_Not.A.Name", "inbound_webhook"),
        ];

        return HttpResponse.json({ connections, total_entries: connections.length });
      }),
      http.get("/api/v2/webhooks/:webhookName/deliveries", ({ params }) => {
        requestedWebhook = String(params.webhookName);

        return HttpResponse.json({ deliveries: [], total_entries: 0 });
      }),
    );

    render(<AppWrapper initialEntries={["/connections"]} />);

    const webhookRow = await screen.findByRole("row", { name: /webhook_demo/u });

    for (const connectionId of ["webhook_http", "unprefixed", "webhook_", "webhook_Not.A.Name"]) {
      const row = screen.getByText(connectionId, { exact: true }).closest("tr");

      expect(row).not.toBeNull();
      expect(within(row as HTMLElement).queryByRole("link")).toBeNull();
    }

    fireEvent.click(within(webhookRow).getByRole("link"));

    await waitFor(() => expect(requestedWebhook).toBe("demo"));
  });
});
