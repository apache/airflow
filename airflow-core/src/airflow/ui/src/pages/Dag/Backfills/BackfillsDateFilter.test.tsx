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
import { render, screen, waitFor, within } from "@testing-library/react";
import { describe, expect, it } from "vitest";

import { AppWrapper } from "src/utils/AppWrapper";

// The backfills mock handler (see src/mocks/handlers/backfills.ts) returns:
//   - id 1 (start_date: 2024-12-01, end_date: 2024-12-31) — excluded when filtering Jan 2025
//   - id 2 (start_date: 2025-01-01, end_date: 2025-01-15) — included when filtering Jan 2025

const getFromDateCells = () =>
  screen.getAllByTestId("time-display").map((element) => element.getAttribute("datetime"));

describe("Backfills start/end date filter", () => {
  it("shows all backfills when no date filter is applied", async () => {
    render(<AppWrapper initialEntries={["/dags/tutorial_taskflow_api/backfills"]} />);

    await waitFor(() => expect(getFromDateCells()).toContain("2025-01-01T00:00:00Z"));
    expect(getFromDateCells()).toContain("2024-12-01T00:00:00Z");
  });

  it("filters backfills by from_date_gte and to_date_lte URL params", async () => {
    render(
      <AppWrapper
        initialEntries={[
          "/dags/tutorial_taskflow_api/backfills?from_date_gte=2025-01-01T00%3A00%3A00Z&to_date_lte=2025-01-31T23%3A59%3A59Z",
        ]}
      />,
    );

    await waitFor(() => expect(getFromDateCells()).toContain("2025-01-01T00:00:00Z"));
    expect(getFromDateCells()).not.toContain("2024-12-01T00:00:00Z");
  });
});

const getTableDateCells = () =>
  within(screen.getByRole("table"))
    .getAllByTestId("time-display")
    .map((element) => element.getAttribute("datetime"));

describe("Backfills status filter", () => {
  it.each([
    { expected: "2025-02-01T00:00:00Z", param: "active=true", unexpected: "2025-01-01T00:00:00Z" },
    { expected: "2025-01-01T00:00:00Z", param: "active=false", unexpected: "2025-02-01T00:00:00Z" },
  ])("filters backfills by $param", async ({ expected, param, unexpected }) => {
    render(<AppWrapper initialEntries={[`/dags/tutorial_taskflow_api/backfills?${param}`]} />);

    // Both conditions in one waitFor: the list keeps the previous rows on screen while the new
    // query resolves, so the absence only holds once that has landed.
    // Scoped to the table: a running backfill also drives a banner elsewhere on the Dag page,
    // whose dates would otherwise count as rows.
    await waitFor(() => {
      expect(getTableDateCells()).toContain(expected);
      expect(getTableDateCells()).not.toContain(unexpected);
    });
  });

  it("shows running and completed backfills together when the filter is unset", async () => {
    render(<AppWrapper initialEntries={["/dags/tutorial_taskflow_api/backfills"]} />);

    await waitFor(() => expect(getTableDateCells()).toContain("2025-02-01T00:00:00Z"));
    expect(getTableDateCells()).toContain("2025-01-01T00:00:00Z");
  });
});
