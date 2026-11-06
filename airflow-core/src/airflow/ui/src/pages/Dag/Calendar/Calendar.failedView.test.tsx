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
import { fireEvent, render, screen } from "@testing-library/react";
import dayjs from "dayjs";
import { MemoryRouter, Route, Routes } from "react-router-dom";
import { beforeEach, describe, expect, it, vi } from "vitest";

import type * as OpenapiQueries from "openapi/queries";
import type { CalendarTimeRangeResponse } from "openapi/requests/types.gen";

import { TIMEZONE_KEY } from "src/constants/localStorage";
import { TimezoneProvider } from "src/context/timezone";
import { BaseWrapper } from "src/utils/Wrapper";

import { Calendar } from "./Calendar";

// `calendar-date` pins the displayed month, so the result doesn't depend on today's date or timezone.
const dayInMonth = dayjs.utc("2025-03-10T00:00:00Z");
const hourIso = (hour: number) => dayInMonth.add(hour, "hour").toISOString();

const dagRuns: Array<CalendarTimeRangeResponse> = [
  { count: 1, date: hourIso(1), state: "success" },
  { count: 1, date: hourIso(3), state: "failed" },
  { count: 1, date: hourIso(5), state: "success" },
  { count: 1, date: hourIso(5), state: "failed" },
];

vi.mock("openapi/queries", async (importOriginal) => ({
  ...(await importOriginal<typeof OpenapiQueries>()),
  useCalendarServiceGetCalendar: vi.fn(() => ({
    data: { dag_runs: dagRuns, total_entries: dagRuns.length },
    error: null,
    isLoading: false,
  })),
  useCalendarServiceGetCalendarDeadlines: vi.fn(() => ({ data: { deadlines: [] } })),
  useDagServiceGetDagDetails: vi.fn(() => ({ data: undefined })),
}));

const renderCalendar = () =>
  render(
    <BaseWrapper>
      <MemoryRouter
        initialEntries={["/dags/calendar_dag/calendar?calendar-granularity=hourly&calendar-date=2025-03-10"]}
      >
        <TimezoneProvider>
          <Routes>
            <Route element={<Calendar />} path="/dags/:dagId/calendar" />
          </Routes>
        </TimezoneProvider>
      </MemoryRouter>
    </BaseWrapper>,
  );

const getCells = () => screen.getAllByTestId("calendar-cell");

const getActiveCells = () => getCells().filter((cell) => cell.dataset.hasData === "true");

const getStates = (cells: Array<HTMLElement>) =>
  cells.flatMap((cell) => (cell.dataset.states ?? "").split(" ").filter(Boolean));

const findCellIndex = (states: string) => getCells().findIndex((cell) => cell.dataset.states === states);

const switchToFailedView = () => fireEvent.click(screen.getByRole("button", { name: /failed/iu }));

describe("Calendar failed view", () => {
  beforeEach(() => {
    localStorage.clear();
    localStorage.setItem(TIMEZONE_KEY, JSON.stringify("UTC"));
  });

  it("shows only failed runs and fewer active cells in the failed view", () => {
    renderCalendar();

    expect(screen.getByTestId("calendar-hourly-view")).toBeInTheDocument();

    const totalCells = getActiveCells();

    expect(totalCells).toHaveLength(3);
    expect(getStates(totalCells)).toEqual(expect.arrayContaining(["success", "failed"]));

    const successOnlyIndex = findCellIndex("success");

    expect(successOnlyIndex).toBeGreaterThanOrEqual(0);

    switchToFailedView();

    const failedCells = getActiveCells();

    expect(failedCells).toHaveLength(2);
    expect(getStates(failedCells)).toContain("failed");
    expect(getStates(failedCells)).not.toContain("success");
    expect(getCells()[successOnlyIndex]).toHaveAttribute("data-has-data", "false");
  });

  it("marks every cell with the failed view mode", () => {
    renderCalendar();

    for (const cell of getCells()) {
      expect(cell).toHaveAttribute("data-view-mode", "total");
    }

    switchToFailedView();

    for (const cell of getCells()) {
      expect(cell).toHaveAttribute("data-view-mode", "failed");
    }
  });

  it("drops the success color from mixed cells in the failed view", () => {
    renderCalendar();

    const mixedIndex = findCellIndex("failed success");

    expect(mixedIndex).toBeGreaterThanOrEqual(0);
    // Mixed success/failed cells render as two diagonal color layers.
    expect(getCells()[mixedIndex]?.children).toHaveLength(2);

    switchToFailedView();

    expect(getCells()[mixedIndex]).toHaveAttribute("data-states", "failed");
    expect(getCells()[mixedIndex]?.children).toHaveLength(0);
  });
});
