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
import type { HttpHandler } from "msw";
import { setupServer, type SetupServerApi } from "msw/node";
import { beforeEach, beforeAll, afterAll, afterEach, vi } from "vitest";

import { handlers } from "src/mocks/handlers";

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

let server: SetupServerApi;

beforeAll(() => {
  server = setupServer(...(handlers as Array<HttpHandler>));
  server.listen({ onUnhandledRequest: "bypass" });
});

beforeEach(() => server.resetHandlers());
afterEach(() => {
  cleanup();
});
afterAll(() => server.close());
