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
import { render, screen } from "@testing-library/react";
import { http, HttpResponse } from "msw";
import { createMemoryRouter, RouterProvider } from "react-router-dom";
import { beforeAll, beforeEach, describe, expect, it } from "vitest";

import { TimezoneProvider } from "src/context/timezone";
import { server } from "src/mocks/server";

import { Logs } from "./Logs";

const logLines = [
  { event: "Trigger 3 starting", level: "info", timestamp: "2026-10-01T00:01:00Z" },
  {
    event: "ConnectionError: broker.example.com:9092 is unreachable",
    level: "error",
    timestamp: "2026-10-01T00:01:01Z",
  },
];

// No retries, so a 403 renders at once instead of after the default backoff.
const renderAt = (path: string) =>
  render(
    <ChakraProvider value={defaultSystem}>
      <QueryClientProvider client={new QueryClient({ defaultOptions: { queries: { retry: false } } })}>
        <TimezoneProvider>
          <RouterProvider
            router={createMemoryRouter([{ element: <Logs />, path: "/triggers/:triggerId/logs" }], {
              initialEntries: [path],
            })}
          />
        </TimezoneProvider>
      </QueryClientProvider>
    </ChakraProvider>,
  );

beforeAll(() => {
  // The log viewer is virtualized and renders no lines in a zero-sized container.
  Object.defineProperty(HTMLElement.prototype, "offsetHeight", { value: 20 });
  Object.defineProperty(HTMLElement.prototype, "clientHeight", { value: 20 });
  Object.defineProperty(HTMLElement.prototype, "offsetWidth", { value: 800 });
});

beforeEach(() => {
  server.use(http.get("/api/v2/triggers/:triggerId/logs", () => HttpResponse.json({ content: logLines })));
});

// Tests render i18n keys: the msw handlers serve empty translation bundles.
describe("Trigger Logs tab", () => {
  it("renders a watcher's own log lines, with no owner log to link", async () => {
    renderAt("/triggers/3/logs");

    expect(await screen.findByText(/Trigger 3 starting/u)).toBeInTheDocument();
    expect(screen.getByText(/broker\.example\.com:9092 is unreachable/u)).toBeInTheDocument();
    expect(screen.queryByRole("link", { name: "logs.trigger.openOwnerLog" })).not.toBeInTheDocument();
  });

  it.each([
    { kind: "task", link: "/dags/deferring_dag/runs/manual_run/tasks/wait/logs", triggerId: 1 },
    { kind: "callback", link: "/dags/deadline_dag/runs/scheduled_run/callbacks", triggerId: 2 },
  ])("links a $kind trigger's lines to its owner's full log", async ({ link, triggerId }) => {
    renderAt(`/triggers/${triggerId}/logs`);

    expect(await screen.findByRole("link", { name: "logs.trigger.openOwnerLog" })).toHaveAttribute(
      "href",
      link,
    );
  });

  it("explains a 403", async () => {
    server.use(
      http.get("/api/v2/triggers/:triggerId/logs", () =>
        HttpResponse.json({ detail: "Reading a trigger's logs needs task log access" }, { status: 403 }),
      ),
    );

    renderAt("/triggers/3/logs");

    expect(await screen.findByText("logs.trigger.forbidden")).toBeInTheDocument();
  });
});
