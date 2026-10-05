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
import { fireEvent, render, screen } from "@testing-library/react";
import dayjs from "dayjs";
import { MemoryRouter, Route, Routes, useLocation } from "react-router-dom";
import { beforeEach, describe, expect, it, vi } from "vitest";

import { TimezoneProvider } from "src/context/timezone";
import { BaseWrapper } from "src/utils/Wrapper";

import { Calendar } from "./Calendar";

const mocks = vi.hoisted(() => ({
  getCalendar: vi.fn(),
  getDagDetails: vi.fn(),
  getDeadlines: vi.fn(),
}));

vi.mock("openapi/queries", () => ({
  useCalendarServiceGetCalendar: mocks.getCalendar,
  useCalendarServiceGetCalendarDeadlines: mocks.getDeadlines,
  useDagServiceGetDagDetails: mocks.getDagDetails,
}));

// Return translation keys as-is so the tests can look elements up by key.
vi.mock("react-i18next", () => ({
  useTranslation: () => ({
    i18n: { language: "en" },
    // eslint-disable-next-line id-length
    t: (key: string) => key,
  }),
}));

// The grid views are not under test here. Stub them so they only expose the
// viewMode prop, which lets us check that the URL state reaches the children.
vi.mock("./DailyCalendarView", () => ({
  DailyCalendarView: ({ viewMode }: { readonly viewMode: string }) => (
    <div data-testid="daily-view">{viewMode}</div>
  ),
}));

vi.mock("./HourlyCalendarView", () => ({
  HourlyCalendarView: ({ viewMode }: { readonly viewMode: string }) => (
    <div data-testid="hourly-view">{viewMode}</div>
  ),
}));

vi.mock("./CalendarLegend", () => ({ CalendarLegend: () => undefined }));

// Renders the current query string so tests can assert on URL changes.
const LocationDisplay = () => <span data-testid="search">{useLocation().search}</span>;

const renderCalendar = (search = "") =>
  render(
    <BaseWrapper>
      <MemoryRouter initialEntries={[`/dags/example_dag/calendar${search}`]}>
        <TimezoneProvider>
          <Routes>
            <Route
              element={
                <>
                  <Calendar />
                  <LocationDisplay />
                </>
              }
              path="/dags/:dagId/calendar"
            />
          </Routes>
        </TimezoneProvider>
      </MemoryRouter>
    </BaseWrapper>,
  );

describe("Calendar URL params", () => {
  beforeEach(() => {
    mocks.getCalendar.mockReset();
    mocks.getDeadlines.mockReset();
    mocks.getDagDetails.mockReset();
    mocks.getCalendar.mockReturnValue({ data: { dag_runs: [] }, error: undefined, isLoading: false });
    mocks.getDeadlines.mockReturnValue({ data: { deadlines: [] } });
    mocks.getDagDetails.mockReturnValue({ data: { timetable_partitioned: false } });
  });

  it("uses the default view (hourly, total runs) when there are no params", () => {
    renderCalendar();

    expect(screen.getByTestId("hourly-view")).toHaveTextContent("total");
    expect(screen.queryByTestId("daily-view")).not.toBeInTheDocument();
  });

  it("reads granularity, view mode and date from the URL", () => {
    renderCalendar("?calendar-granularity=daily&calendar-view-mode=failed&calendar-date=2026-07-26");

    expect(screen.getByTestId("daily-view")).toHaveTextContent("failed");
    expect(screen.getByTestId("calendar-current-period")).toHaveTextContent("2026");
    expect(mocks.getCalendar).toHaveBeenLastCalledWith(
      expect.objectContaining({ granularity: "daily" }),
      undefined,
      expect.anything(),
    );
  });

  it("shows the month from calendar-date in hourly mode", () => {
    renderCalendar("?calendar-granularity=hourly&calendar-date=2026-07-26");

    expect(screen.getByTestId("hourly-view")).toBeInTheDocument();
    expect(screen.getByTestId("calendar-current-period")).toHaveTextContent("Jul 2026");
  });

  it("falls back to defaults for invalid param values", () => {
    renderCalendar("?calendar-granularity=abc&calendar-view-mode=xyz&calendar-date=not-a-date");

    expect(screen.getByTestId("hourly-view")).toHaveTextContent("total");
    // An invalid date falls back to the current month, not "Invalid Date".
    expect(screen.getByTestId("calendar-current-period")).toHaveTextContent(dayjs().format("MMM YYYY"));
  });

  it("updates the URL when toggling granularity and view mode", () => {
    renderCalendar();

    fireEvent.click(screen.getByText("calendar.daily"));
    expect(screen.getByTestId("search")).toHaveTextContent("calendar-granularity=daily");

    fireEvent.click(screen.getByText("overview.buttons.failedRun_other"));
    expect(screen.getByTestId("search")).toHaveTextContent("calendar-view-mode=failed");
    // Changing one param must keep the others.
    expect(screen.getByTestId("search")).toHaveTextContent("calendar-granularity=daily");
  });

  it("updates calendar-date when navigating to the next year", () => {
    renderCalendar("?calendar-granularity=daily&calendar-date=2026-07-26");

    fireEvent.click(screen.getByRole("button", { name: "calendar.navigation.nextYear" }));
    expect(screen.getByTestId("search")).toHaveTextContent("calendar-date=2027-07-26");
  });
});
