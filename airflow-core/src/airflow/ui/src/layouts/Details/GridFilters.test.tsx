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
import type { PropsWithChildren } from "react";

import "@testing-library/jest-dom";
import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import dayjs from "dayjs";
import timezone from "dayjs/plugin/timezone";
import utc from "dayjs/plugin/utc";
import { MemoryRouter } from "react-router-dom";
import { afterEach, describe, expect, it } from "vitest";

import { TimezoneProvider } from "src/context/timezone";
import { BaseWrapper } from "src/utils/Wrapper";

import { GridFilters } from "./GridFilters";

dayjs.extend(timezone);
dayjs.extend(utc);

const createWrapper =
  (initialEntries: Array<string> = ["/dags/test_dag/runs/run_1"]) =>
  ({ children }: PropsWithChildren) => (
    <BaseWrapper>
      <MemoryRouter initialEntries={initialEntries}>
        <TimezoneProvider>{children}</TimezoneProvider>
      </MemoryRouter>
    </BaseWrapper>
  );

afterEach(cleanup);

describe("GridFilters gantt date filters", () => {
  it("offers the start and end date filters when enabled", async () => {
    render(<GridFilters showGanttDateFilters />, { wrapper: createWrapper() });

    fireEvent.click(screen.getByTestId("add-filter-button"));

    expect(await screen.findByTestId("add-filter-start_date_range")).toBeInTheDocument();
    expect(screen.getByTestId("add-filter-end_date_range")).toBeInTheDocument();
  });

  it("does not offer the date filters by default", async () => {
    render(<GridFilters />, { wrapper: createWrapper() });

    fireEvent.click(screen.getByTestId("add-filter-button"));

    expect(await screen.findByTestId("add-filter-run_id_pattern")).toBeInTheDocument();
    expect(screen.queryByTestId("add-filter-start_date_range")).not.toBeInTheDocument();
    expect(screen.queryByTestId("add-filter-end_date_range")).not.toBeInTheDocument();
  });

  it("restores active date filters from the URL", () => {
    render(<GridFilters showGanttDateFilters />, {
      wrapper: createWrapper([
        "/dags/test_dag/runs/run_1?start_date_gte=2024-03-14T10:00:00Z&end_date_lte=2024-03-14T11:00:00Z",
      ]),
    });

    expect(screen.getByTestId("start_date_range-pill")).toBeInTheDocument();
    expect(screen.getByTestId("end_date_range-pill")).toBeInTheDocument();
  });
});
