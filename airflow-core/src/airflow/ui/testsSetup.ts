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
import { cleanup } from "@testing-library/react";
import type { SetupServer } from "msw/node";
import { beforeEach, afterEach, vi } from "vitest";

import { server as sharedServer } from "src/mocks/server";

// tsconfig.dev.json, which covers this file, has no `src/*` path alias, so the import is untyped here.
const server = sharedServer as SetupServer;

// ELK creates a Worker at module-load time, which is not available in
// happy-dom. Mock useGraphLayout so the Worker is never constructed. Any
// test that specifically exercises graph layout should override this mock.
vi.mock("src/components/Graph/useGraphLayout", () => ({
  useGraphLayout: vi.fn().mockReturnValue({ data: undefined, isPending: false }),
}));

// Render nothing for the two graph-rendering components so tests that render
// full pages don't need to stub layout data.
vi.mock("src/layouts/Details/Graph/Graph", () => ({ Graph: () => null }));
vi.mock("src/pages/Asset/AssetGraph", () => ({ AssetGraph: () => null }));

// Mock Chart.js to prevent DOM access errors during test cleanup
vi.mock("react-chartjs-2", () => ({
  Bar: vi.fn(() => {
    // eslint-disable-next-line @typescript-eslint/no-require-imports, @typescript-eslint/no-unsafe-assignment
    const React = require("react");

    // eslint-disable-next-line @typescript-eslint/no-unsafe-return, @typescript-eslint/no-unsafe-call, @typescript-eslint/no-unsafe-member-access
    return React.createElement("div", { "data-testid": "mock-chart" });
  }),
  Line: vi.fn(() => {
    // eslint-disable-next-line @typescript-eslint/no-require-imports, @typescript-eslint/no-unsafe-assignment
    const React = require("react");

    // eslint-disable-next-line @typescript-eslint/no-unsafe-return, @typescript-eslint/no-unsafe-call, @typescript-eslint/no-unsafe-member-access
    return React.createElement("div", { "data-testid": "mock-chart" });
  }),
}));

// Mock Chart.js core to prevent initialization errors
vi.mock("chart.js", () => ({
  BarElement: vi.fn(),
  CategoryScale: vi.fn(),
  Chart: {
    register: vi.fn(),
  },
  Filler: vi.fn(),
  Legend: vi.fn(),
  LinearScale: vi.fn(),
  LineElement: vi.fn(),
  PointElement: vi.fn(),
  TimeScale: vi.fn(),
  Title: vi.fn(),
  Tooltip: vi.fn(),
}));

// Listen at import time rather than in beforeAll: modules such as src/i18n/config send requests
// while the test file is still being imported.
//
// The server is deliberately never closed. Vitest runs each test file in its own worker process,
// which takes the interception down with it, whereas closing in afterAll releases sockets of
// mocked requests that are still open to the real network, where they fail with ECONNREFUSED or
// "socket hang up".
server.listen({ onUnhandledFrame: "error" });

// The "error" strategy only rejects the request, which the app under test may well swallow, so
// also fail the test that sent it.
const unhandledRequests: Array<string> = [];

server.events.on("request:unhandled", ({ request }) => {
  unhandledRequests.push(`${request.method} ${request.url}`);
});

beforeEach(() => server.resetHandlers());
afterEach(() => {
  cleanup();

  const requests = unhandledRequests.splice(0);

  if (requests.length > 0) {
    throw new Error(
      `Requests without an msw handler (add one to src/mocks/handlers or with server.use()):\n${requests.join("\n")}`,
    );
  }
});
