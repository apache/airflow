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
import { render, screen, fireEvent, waitFor, cleanup } from "@testing-library/react";
import dayjs from "dayjs";
import timezone from "dayjs/plugin/timezone";
import utc from "dayjs/plugin/utc";
import { useMemo } from "react";
import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";

import { TimezoneContext } from "src/context/timezone";
import { ChakraWrapper } from "src/utils/ChakraWrapper";

import type { FilterPluginProps } from "../types";
import { DateRangeFilter } from "./DateRangeFilter";

dayjs.extend(timezone);
dayjs.extend(utc);

const mockTranslate = vi.fn((key: string) => {
  const translations: Record<string, string> = {
    "common:filters.endTime": "End Time",
    "common:filters.selectDateRange": "Select Date Range",
    "common:filters.startTime": "Start Time",
    "common:table.from": "From",
    "common:table.to": "To",
    "components:dateRangeFilter.validation.invalidDateFormat": "Invalid date format.",
    "components:dateRangeFilter.validation.invalidTimeFormat": "Invalid time format.",
    "components:dateRangeFilter.validation.startBeforeEnd": "Start date/time must be before end date/time",
  };

  return translations[key] ?? key;
});

vi.mock("react-i18next", () => ({
  useTranslation: () => ({
    i18n: { language: "en" },
    // eslint-disable-next-line id-length
    t: mockTranslate,
  }),
}));

const TestWrapper = ({ children }: { readonly children: React.ReactNode }) => {
  const timezoneContextValue = useMemo(
    () => ({
      availableTimezones: ["UTC", "America/New_York"],
      selectedTimezone: "UTC",
      setSelectedTimezone: vi.fn(),
    }),
    [],
  );

  return (
    <ChakraWrapper>
      <TimezoneContext.Provider value={timezoneContextValue}>{children}</TimezoneContext.Provider>
    </ChakraWrapper>
  );
};

const mockFilter = {
  config: {
    icon: undefined,
    key: "dateRange",
    label: "Date Range",
    type: "daterange" as const,
  },
  id: "test-filter",
  value: undefined,
};

const defaultProps: FilterPluginProps = {
  filter: mockFilter,
  onChange: vi.fn(),
  onRemove: vi.fn(),
};

const getInputs = () => {
  const dateInputs = screen.getAllByPlaceholderText("YYYY/MM/DD");
  const timeInputs = screen.getAllByPlaceholderText("HH:mm");

  return {
    endDateInput: dateInputs[1],
    endTimeInput: timeInputs[1],
    startDateInput: dateInputs[0],
    startTimeInput: timeInputs[0],
  };
};

const changeDateInput = (input: HTMLElement | undefined, value: string) => {
  if (input) {
    fireEvent.change(input, { target: { value } });
  }
};

const changeTimeInput = (input: HTMLElement | undefined, value: string) => {
  if (input) {
    fireEvent.change(input, { target: { value } });
  }
};

const waitForError = async (errorText: string) => {
  await waitFor(() => {
    expect(screen.getByText(errorText)).toBeInTheDocument();
  });
};

const waitForNoError = async (errorText: string) => {
  await waitFor(() => {
    expect(screen.queryByText(errorText)).not.toBeInTheDocument();
  });
};

const waitForNoErrors = async (errorTexts: Array<string>) => {
  await waitFor(() => {
    for (const errorText of errorTexts) {
      expect(screen.queryByText(errorText)).not.toBeInTheDocument();
    }
  });
};

const renderFilter = (props: FilterPluginProps = defaultProps) =>
  render(
    <TestWrapper>
      <DateRangeFilter {...props} />
    </TestWrapper>,
  );

describe("DateRangeFilter", () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  afterEach(() => {
    cleanup();
  });

  describe("Input Validation", () => {
    it("validates date and time formats", async () => {
      renderFilter();
      const { startDateInput, startTimeInput } = getInputs();

      changeDateInput(startDateInput, "invalid-date");
      await waitForError("Invalid date format.");
      changeDateInput(startDateInput, "2024/13/01");
      await waitForError("Invalid date format.");
      changeTimeInput(startTimeInput, "25:00");
      await waitForError("Invalid time format.");
    });

    it("validates date range", async () => {
      renderFilter();
      const { endDateInput, endTimeInput, startDateInput, startTimeInput } = getInputs();

      changeDateInput(startDateInput, "2024/01/15");
      changeTimeInput(startTimeInput, "10:00");
      changeDateInput(endDateInput, "2024/01/14");
      changeTimeInput(endTimeInput, "09:00");
      await waitForError("Start date/time must be before end date/time");
    });

    it("accepts valid inputs", async () => {
      renderFilter();
      const { endDateInput, endTimeInput, startDateInput, startTimeInput } = getInputs();

      changeDateInput(startDateInput, "2024/01/15");
      changeTimeInput(startTimeInput, "09:00");
      changeDateInput(endDateInput, "2024/01/20");
      changeTimeInput(endTimeInput, "17:00");
      await waitForNoErrors(["Invalid date format.", "Start date/time must be before end date/time"]);
    });
  });

  describe("Display Value Formatting", () => {
    it("displays placeholder when no value is set", () => {
      renderFilter();
      expect(screen.getByText("Select Date Range")).toBeInTheDocument();
    });

    it("displays formatted date range", () => {
      const props = {
        ...defaultProps,
        filter: {
          ...mockFilter,
          value: { endDate: "2024-01-20T17:00:00Z", startDate: "2024-01-15T09:00:00Z" },
        },
      };

      renderFilter(props);
      expect(screen.getByText(/Jan 15, 2024/u)).toBeInTheDocument();
    });

    it("displays 'From' when only start date is set", () => {
      const props = {
        ...defaultProps,
        filter: { ...mockFilter, value: { endDate: undefined, startDate: "2024-01-15T09:00:00Z" } },
      };

      renderFilter(props);
      expect(screen.getAllByText(/From/u).length).toBeGreaterThan(0);
    });

    it("displays 'To' when only end date is set", () => {
      const props = {
        ...defaultProps,
        filter: { ...mockFilter, value: { endDate: "2024-01-20T17:00:00Z", startDate: undefined } },
      };

      renderFilter(props);
      expect(screen.getAllByText(/To/u).length).toBeGreaterThan(0);
    });
  });

  describe("Edge Cases", () => {
    it("handles leap years and boundary dates", async () => {
      renderFilter();
      const { endDateInput, startDateInput } = getInputs();

      changeDateInput(startDateInput, "2024/02/29");
      await waitForNoError("Invalid date format.");
      changeDateInput(startDateInput, "2023/02/29");
      await waitForError("Invalid date format.");
      changeDateInput(startDateInput, "2024/01/31");
      changeDateInput(endDateInput, "2024/02/01");
      await waitForNoError("Invalid date format.");
    });

    it("handles undefined filter value", () => {
      const props = { ...defaultProps, filter: { ...mockFilter, value: undefined } };

      // Should not throw when filter value is undefined
      renderFilter(props);
    });
  });
});
