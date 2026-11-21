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
import { render, screen, waitFor } from "@testing-library/react";
import { http, HttpResponse } from "msw";
import { Outlet, RouterProvider, createMemoryRouter } from "react-router-dom";
import { describe, expect, it, vi } from "vitest";

import { server } from "src/mocks/server";

import { DagDetailsRoute } from "./DagDetailsRoute";

// The real layout needs the Grid's queries; only what it is handed matters here.
vi.mock("./DetailsLayout", () => ({
  DetailsLayout: ({ error }: { readonly error?: unknown }) => (
    <div data-testid="layout">
      <div data-testid="chip-error">{error instanceof Error ? error.message : ""}</div>
      <Outlet />
    </div>
  ),
}));

const Level = () => <div data-testid="level" />;

const renderRoute = (initialPath: string) => {
  const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  const router = createMemoryRouter(
    [
      {
        children: [
          { element: <Level />, path: "runs/:runId" },
          { element: <Level />, path: "runs/:runId/tasks/:taskId" },
        ],
        element: <DagDetailsRoute />,
        path: "dags/:dagId",
      },
    ],
    { initialEntries: [initialPath] },
  );

  render(
    <ChakraProvider value={defaultSystem}>
      <QueryClientProvider client={queryClient}>
        <RouterProvider router={router} />
      </QueryClientProvider>
    </ChakraProvider>,
  );
};

describe("DagDetailsRoute", () => {
  // The chip speaks for the Dag as a whole, so it carries the Dag's own failure at every level; a
  // level that fails says so in its own header instead.
  it("puts the Dag's own load failure on the chip", async () => {
    server.use(
      http.get("/api/v2/dags/broken/details", () =>
        HttpResponse.json({ detail: "Dag failed to load" }, { status: 500 }),
      ),
    );

    renderRoute("/dags/broken/runs/run_1");

    expect(await screen.findByTestId("level")).toBeInTheDocument();
    await waitFor(() => expect(screen.getByTestId("chip-error")).not.toBeEmptyDOMElement());
  });

  it("shows the Dag not found page instead of a nested level for a missing Dag", async () => {
    server.use(
      http.get("/api/v2/dags/missing/details", () =>
        HttpResponse.json({ detail: "Dag not found" }, { status: 404 }),
      ),
    );

    renderRoute("/dags/missing/runs/run_1/tasks/task_1");

    expect(await screen.findByText("404")).toBeInTheDocument();
    expect(screen.queryByTestId("layout")).not.toBeInTheDocument();
    expect(screen.queryByTestId("level")).not.toBeInTheDocument();
  });
});
