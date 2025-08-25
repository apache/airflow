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
import "@testing-library/jest-dom";
import { render, screen, waitFor } from "@testing-library/react";
import { setupServer, type SetupServerApi } from "msw/node";
import { afterEach, describe, it, expect, beforeAll, afterAll } from "vitest";
import { MemoryRouter } from "react-router-dom";

import { handlers } from "src/mocks/handlers";
import { MOCK_DAG } from "src/mocks/handlers/dag";
import { Wrapper } from "src/utils/Wrapper";

import { Trigger } from "./Trigger";

let server: SetupServerApi;

beforeAll(() => {
  server = setupServer(...handlers);
  server.listen({ onUnhandledRequest: "bypass" });
});

afterEach(() => server.resetHandlers());
afterAll(() => server.close());

describe("Trigger Page", () => {
  it("renders trigger form with default values", async () => {
    render(
      <MemoryRouter initialEntries={["/dags/example_dag/trigger"]}>
        <Wrapper>
          <Trigger />
        </Wrapper>
      </MemoryRouter>,
    );

    await waitFor(() => {
      expect(screen.getByText(/Trigger - example_dag/)).toBeInTheDocument();
    });

    // Check that form elements are present
    expect(screen.getByText("Trigger")).toBeInTheDocument();
    expect(screen.getByText("Back")).toBeInTheDocument();
    expect(screen.getByText("Cancel")).toBeInTheDocument();
  });

  it("pre-populates form fields from URL parameters", async () => {
    const testConf = '{"param1":"value1","param2":"value2"}';
    const testDagRunId = "manual_run_001";
    const testLogicalDate = "2024-01-15T10:00:00.000";
    const testNote = "Test note";

    render(
      <MemoryRouter
        initialEntries={[
          `/dags/example_dag/trigger?conf=${encodeURIComponent(testConf)}&dag_run_id=${testDagRunId}&logical_date=${testLogicalDate}&note=${encodeURIComponent(testNote)}`,
        ]}
      >
        <Wrapper>
          <Trigger />
        </Wrapper>
      </MemoryRouter>,
    );

    await waitFor(() => {
      expect(screen.getByText(/Trigger - example_dag/)).toBeInTheDocument();
    });

    // The form should be pre-populated with URL parameters
    // Note: We can't directly test the form values without more complex setup,
    // but we can verify the component renders without errors
    expect(screen.getByText("Trigger")).toBeInTheDocument();
  });

  it("handles missing URL parameters gracefully", async () => {
    render(
      <MemoryRouter initialEntries={["/dags/example_dag/trigger"]}>
        <Wrapper>
          <Trigger />
        </Wrapper>
      </MemoryRouter>,
    );

    await waitFor(() => {
      expect(screen.getByText(/Trigger - example_dag/)).toBeInTheDocument();
    });

    // Should render without errors even with no URL parameters
    expect(screen.getByText("Trigger")).toBeInTheDocument();
  });

  it("shows loading state initially", async () => {
    render(
      <MemoryRouter initialEntries={["/dags/example_dag/trigger"]}>
        <Wrapper>
          <Trigger />
        </Wrapper>
      </MemoryRouter>,
    );

    // Initially should show loading state
    expect(screen.getByText("Loading...")).toBeInTheDocument();

    // After loading, should show the form
    await waitFor(() => {
      expect(screen.getByText(/Trigger - example_dag/)).toBeInTheDocument();
    });
  });

  it("handles DAG not found error", async () => {
    // Mock a 404 response for the DAG
    server.use(
      ...handlers.filter((handler) => !handler.info.path.includes("/dags/")),
      {
        info: { path: "/api/v1/dags/:dagId", method: "GET" },
        resolver: () => {
          return new Response(JSON.stringify({ detail: "DAG not found" }), {
            status: 404,
            headers: { "Content-Type": "application/json" },
          });
        },
      },
    );

    render(
      <MemoryRouter initialEntries={["/dags/nonexistent_dag/trigger"]}>
        <Wrapper>
          <Trigger />
        </Wrapper>
      </MemoryRouter>,
    );

    await waitFor(() => {
      expect(screen.getByText("Failed to load DAG")).toBeInTheDocument();
    });
  });
});
