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
import { MemoryRouter, Route, Routes } from "react-router-dom";
import { describe, expect, it } from "vitest";

import { TimezoneProvider } from "src/context/timezone";
import { BaseWrapper } from "src/utils/Wrapper";

import "../../../i18n/config";
import { Backfills } from "./Backfills";

// The backfills mock handler (see src/mocks/handlers/backfills.ts) returns:
//   - id 1 (start_date: 2024-12-01, end_date: 2024-12-31) — excluded when filtering Jan 2025
//   - id 2 (start_date: 2025-01-01, end_date: 2025-01-15) — included when filtering Jan 2025
const renderBackfills = (initialEntry: string) =>
  render(
    <BaseWrapper>
      <MemoryRouter initialEntries={[initialEntry]}>
        <TimezoneProvider>
          <Routes>
            <Route element={<Backfills />} path="dags/:dagId/backfills" />
          </Routes>
        </TimezoneProvider>
      </MemoryRouter>
    </BaseWrapper>,
  );

const getFromDateCells = () =>
  screen.getAllByTestId("time-display").map((element) => element.getAttribute("datetime"));

describe("Backfills start/end date filter", () => {
  it("shows all backfills when no date filter is applied", async () => {
    renderBackfills("/dags/tutorial_taskflow_api/backfills");

    await waitFor(() => expect(getFromDateCells()).toContain("2025-01-01T00:00:00Z"));
    expect(getFromDateCells()).toContain("2024-12-01T00:00:00Z");
  });

  it("filters backfills by start_date_gte and end_date_lte URL params", async () => {
    renderBackfills(
      "/dags/tutorial_taskflow_api/backfills?start_date_gte=2025-01-01T00%3A00%3A00Z&end_date_lte=2025-01-31T23%3A59%3A59Z",
    );

    await waitFor(() => expect(getFromDateCells()).toContain("2025-01-01T00:00:00Z"));
    expect(getFromDateCells()).not.toContain("2024-12-01T00:00:00Z");
  });
});
