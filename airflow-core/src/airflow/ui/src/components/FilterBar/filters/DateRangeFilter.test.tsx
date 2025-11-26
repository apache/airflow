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
import { render, screen, fireEvent, waitFor } from "@testing-library/react";
import dayjs from "dayjs";
import timezone from "dayjs/plugin/timezone";
import utc from "dayjs/plugin/utc";
import { useMemo } from "react";
import { describe, it, expect, vi, beforeEach } from "vitest";

import { TimezoneContext } from "src/context/timezone";
import { ChakraWrapper } from "src/utils/ChakraWrapper";

import type { FilterPluginProps } from "../types";
import { DateRangeFilter } from "./DateRangeFilter";

// Initialize dayjs plugins
dayjs.extend(timezone);
dayjs.extend(utc);

// Mock useTranslation
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

// Mock data
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

describe("DateRangeFilter - Input Validation", () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it("shows error for invalid date format in start date", async () => {
    render(
      <TestWrapper>
        <DateRangeFilter {...defaultProps} />
      </TestWrapper>,
    );

    const [startDateInput] = screen.getAllByPlaceholderText("YYYY/MM/DD");

    if (startDateInput) {
      fireEvent.change(startDateInput, { target: { value: "invalid-date" } });
    }

    await waitFor(() => {
      expect(screen.getByText("Invalid date format.")).toBeInTheDocument();
    });
  });

  it("shows error for invalid date format in end date", async () => {
    render(
      <TestWrapper>
        <DateRangeFilter {...defaultProps} />
      </TestWrapper>,
    );

    const [, endDateInput] = screen.getAllByPlaceholderText("YYYY/MM/DD");

    if (endDateInput) {
      fireEvent.change(endDateInput, { target: { value: "invalid-date" } });
    }

    await waitFor(() => {
      expect(screen.getByText("Invalid date format.")).toBeInTheDocument();
    });
  });

  it("shows error for invalid time format in start time", async () => {
    render(
      <TestWrapper>
        <DateRangeFilter {...defaultProps} />
      </TestWrapper>,
    );

    const [startTimeInput] = screen.getAllByPlaceholderText("HH:mm");

    if (startTimeInput) {
      fireEvent.change(startTimeInput, { target: { value: "invalid-time" } });
    }

    await waitFor(() => {
      expect(screen.getByText("Invalid time format.")).toBeInTheDocument();
    });
  });

  it("shows error for invalid time format in end time", async () => {
    render(
      <TestWrapper>
        <DateRangeFilter {...defaultProps} />
      </TestWrapper>,
    );

    const [, endTimeInput] = screen.getAllByPlaceholderText("HH:mm");

    if (endTimeInput) {
      fireEvent.change(endTimeInput, { target: { value: "invalid-time" } });
    }

    await waitFor(() => {
      expect(screen.getByText("Invalid time format.")).toBeInTheDocument();
    });
  });

  it("shows error when start date/time is after end date/time", async () => {
    render(
      <TestWrapper>
        <DateRangeFilter {...defaultProps} />
      </TestWrapper>,
    );

    const [startDateInput, endDateInput] = screen.getAllByPlaceholderText("YYYY/MM/DD");
    const [startTimeInput, endTimeInput] = screen.getAllByPlaceholderText("HH:mm");

    // Set start date/time to be after end date/time
    if (startDateInput && startTimeInput && endDateInput && endTimeInput) {
      fireEvent.change(startDateInput, { target: { value: "2024/01/15" } });
      fireEvent.change(startTimeInput, { target: { value: "10:00" } });
      fireEvent.change(endDateInput, { target: { value: "2024/01/14" } });
      fireEvent.change(endTimeInput, { target: { value: "09:00" } });
    }

    await waitFor(() => {
      expect(screen.getByText("Start date/time must be before end date/time")).toBeInTheDocument();
    });
  });

  it("accepts valid date and time inputs", async () => {
    render(
      <TestWrapper>
        <DateRangeFilter {...defaultProps} />
      </TestWrapper>,
    );

    const [startDateInput, endDateInput] = screen.getAllByPlaceholderText("YYYY/MM/DD");
    const [startTimeInput, endTimeInput] = screen.getAllByPlaceholderText("HH:mm");

    // Set valid inputs
    if (startDateInput && startTimeInput && endDateInput && endTimeInput) {
      fireEvent.change(startDateInput, { target: { value: "2024/01/15" } });
      fireEvent.change(startTimeInput, { target: { value: "09:00" } });
      fireEvent.change(endDateInput, { target: { value: "2024/01/20" } });
      fireEvent.change(endTimeInput, { target: { value: "17:00" } });
    }

    await waitFor(() => {
      // Should not show any validation errors
      expect(screen.queryByText("Invalid date format.")).not.toBeInTheDocument();
      expect(screen.queryByText("Invalid time format.")).not.toBeInTheDocument();
      expect(screen.queryByText("Start date/time must be before end date/time")).not.toBeInTheDocument();
    });
  });

  it("clears validation errors when invalid input is corrected", async () => {
    render(
      <TestWrapper>
        <DateRangeFilter {...defaultProps} />
      </TestWrapper>,
    );

    const [startDateInput] = screen.getAllByPlaceholderText("YYYY/MM/DD");

    if (startDateInput) {
      // First, set an invalid date
      fireEvent.change(startDateInput, { target: { value: "invalid-date" } });

      await waitFor(() => {
        expect(screen.getByText("Invalid date format.")).toBeInTheDocument();
      });

      // Then, correct it to a valid date
      fireEvent.change(startDateInput, { target: { value: "2024/01/15" } });
    }

    await waitFor(() => {
      expect(screen.queryByText("Invalid date format.")).not.toBeInTheDocument();
    });
  });
});
