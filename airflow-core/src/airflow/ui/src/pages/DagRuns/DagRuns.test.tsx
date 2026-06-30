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
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { AppWrapper } from "src/utils/AppWrapper";

vi.mock("src/components/RenderedJsonField", () => ({
  default: ({ content }: { content: object }) => (
    <div data-testid="rendered-json">{JSON.stringify(content)}</div>
  ),
}));

const clearDagRunsTablePreferences = () => {
  globalThis.localStorage.removeItem("dataTable:common:dagRun:columnVisibility");
};

// The dag_runs mock handler (see src/mocks/handlers/dag_runs.ts) returns:
//   - run_before_filter (logical_date: 2024-12-31) — excluded when filtering Jan 2025
//   - run_in_range      (logical_date: 2025-01-15) — included when filtering Jan 2025
describe("DagRuns logical date filter", () => {
  it("shows all runs when no logical date filter is applied", async () => {
    render(<AppWrapper initialEntries={["/dag_runs"]} />);

    await waitFor(() => expect(screen.getByText("run_in_range")).toBeInTheDocument());
    expect(screen.getByText("run_before_filter")).toBeInTheDocument();
  });

  it("filters runs by logical_date_gte and logical_date_lte URL params", async () => {
    render(
      <AppWrapper
        initialEntries={[
          "/dag_runs?logical_date_gte=2025-01-01T00%3A00%3A00Z&logical_date_lte=2025-01-31T23%3A59%3A59Z",
        ]}
      />,
    );

    await waitFor(() => expect(screen.getByText("run_in_range")).toBeInTheDocument());
    expect(screen.queryByText("run_before_filter")).not.toBeInTheDocument();
  });
});

describe("DagRuns conf column", () => {
  beforeEach(() => {
    clearDagRunsTablePreferences();
  });

  afterEach(() => {
    clearDagRunsTablePreferences();
  });

  it("shows the conf column by default on the Dag Runs list", async () => {
    render(<AppWrapper initialEntries={["/dag_runs"]} />);

    await waitFor(() => expect(screen.getByText("run_in_range")).toBeInTheDocument());
    expect(screen.getByRole("columnheader", { name: "dagRun.conf sortedUnsorted" })).toBeInTheDocument();
  });

  it("renders run conf values when present", async () => {
    render(<AppWrapper initialEntries={["/dag_runs"]} />);

    await waitFor(() => expect(screen.getByText("run_in_range")).toBeInTheDocument());
    expect(screen.getByTestId("rendered-json")).toHaveTextContent('{"env":"prod"}');
  });

  it("filters runs by conf_contains URL param", async () => {
    render(<AppWrapper initialEntries={["/dag_runs?conf_contains=prod"]} />);

    await waitFor(() => expect(screen.getByText("run_in_range")).toBeInTheDocument());
    expect(screen.queryByText("run_before_filter")).not.toBeInTheDocument();
    expect(screen.queryByText("run_empty_conf")).not.toBeInTheDocument();
  });
});
