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
import { describe, it, expect, vi, beforeEach } from "vitest";

// Initialize dayjs plugins
dayjs.extend(timezone);
dayjs.extend(utc);

import { useMemo } from "react";

import { TimezoneContext } from "src/context/timezone";
import { ChakraWrapper } from "src/utils/ChakraWrapper";

import { DateRangeFilter } from "./DateRangeFilter";
import type { DateRangeValue, FilterPluginProps } from "../types";

// Mock useTranslation
const mockTranslate = vi.fn((key: string) => {
  const translations: Record<string, string> = {
    "components:dateRangeFilter.validation.invalidDateFormat": "Invalid date format.",
    "components:dateRangeFilter.validation.invalidTimeFormat": "Invalid time format.",
    "components:dateRangeFilter.validation.startBeforeEnd": "Start date/time must be before end date/time",
    "common:filters.endTime": "End Time",
    "common:filters.from": "From",
    "common:filters.selectDateRange": "Select Date Range",
    "common:filters.startTime": "Start Time",
    "common:filters.to": "To",
    "common:table.from": "From",
    "common:table.to": "To",
  };

  return translations[key] ?? key;
});

vi.mock("react-i18next", () => ({
  useTranslation: () => ({
    // eslint-disable-next-line id-length
    t: mockTranslate,
  }),
}));

const mockFilter = {
  config: {
    key: "dateRange",
    label: "Date Range",
    type: "daterange" as const,
  },
  id: "test-date-range",
  value: undefined,
};

const mockOnChange = vi.fn();
const mockOnRemove = vi.fn();

const defaultProps: FilterPluginProps = {
  filter: mockFilter,
  onChange: mockOnChange,
  onRemove: mockOnRemove,
};

const TestWrapper = ({ children, testTimezone = "UTC" }: { readonly children: React.ReactNode; readonly testTimezone?: string }) => {
  const contextValue = useMemo(() => ({
    selectedTimezone: testTimezone,
    setSelectedTimezone: vi.fn(),
  }), [testTimezone]);

  return (
    <ChakraWrapper>
      <TimezoneContext.Provider value={contextValue}>
        {children}
      </TimezoneContext.Provider>
    </ChakraWrapper>
  );
};

describe("DateRangeFilter", () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  describe("Display and Rendering", () => {
    it("renders filter label and placeholder when no value is set", () => {
      render(
        <TestWrapper>
          <DateRangeFilter {...defaultProps} />
        </TestWrapper>
      );

      expect(screen.getByText("Date Range:")).toBeInTheDocument();
      expect(screen.getByText("Select Date Range")).toBeInTheDocument();
    });

    it("renders formatted date range when both start and end dates are set", () => {
      const value: DateRangeValue = {
        endDate: "2024-01-20T15:45:00Z",
        startDate: "2024-01-15T10:30:00Z",
      };

      render(
        <TestWrapper>
          <DateRangeFilter {...defaultProps} filter={{ ...mockFilter, value }} />
        </TestWrapper>
      );

      expect(screen.getByText(/Jan 15, 2024 10:30 - Jan 20, 2024 15:45/u)).toBeInTheDocument();
    });

    it("renders single date format when only start date is set", () => {
      const value: DateRangeValue = {
        endDate: undefined,
        startDate: "2024-01-15T10:30:00Z",
      };

      render(
        <TestWrapper>
          <DateRangeFilter {...defaultProps} filter={{ ...mockFilter, value }} />
        </TestWrapper>
      );

      expect(screen.getByText(/From Jan 15, 2024 10:30/u)).toBeInTheDocument();
    });

    it("renders single date format when only end date is set", () => {
      const value: DateRangeValue = {
        endDate: "2024-01-20T15:45:00Z",
        startDate: undefined,
      };

      render(
        <TestWrapper>
          <DateRangeFilter {...defaultProps} filter={{ ...mockFilter, value }} />
        </TestWrapper>
      );

      expect(screen.getByText(/To Jan 20, 2024 15:45/u)).toBeInTheDocument();
    });

    it("renders same-day range in compact format", () => {
      const value: DateRangeValue = {
        endDate: "2024-01-15T15:45:00Z",
        startDate: "2024-01-15T10:30:00Z",
      };

      render(
        <TestWrapper>
          <DateRangeFilter {...defaultProps} filter={{ ...mockFilter, value }} />
        </TestWrapper>
      );

      expect(screen.getByText(/Jan 15, 2024 10:30 - 15:45/u)).toBeInTheDocument();
    });
  });

  describe("Timezone Support", () => {
    it("renders times in the selected timezone", () => {
      const value: DateRangeValue = {
        endDate: "2024-01-20T15:45:00Z",
        startDate: "2024-01-15T10:30:00Z",
      };

      render(
        <TestWrapper testTimezone="America/New_York">
          <DateRangeFilter {...defaultProps} filter={{ ...mockFilter, value }} />
        </TestWrapper>
      );

      // Time should be adjusted for EST/EDT (UTC-5)
      expect(screen.getByText(/05:30.*10:45/u)).toBeInTheDocument();
    });
  });

  describe("Remove Filter", () => {
    it("calls onRemove when remove button is clicked", () => {
      render(
        <TestWrapper>
          <DateRangeFilter {...defaultProps} />
        </TestWrapper>
      );

      const removeButton = screen.getByLabelText("Remove Date Range filter");

      fireEvent.click(removeButton);

      expect(mockOnRemove).toHaveBeenCalledTimes(1);
    });
  });

  describe("Filter States", () => {
    it("shows correct styling for empty filter", () => {
      render(
        <TestWrapper>
          <DateRangeFilter {...defaultProps} />
        </TestWrapper>
      );

      const placeholder = screen.getByText("Select Date Range");

      expect(placeholder).toHaveClass("css-1ij6rp6"); // gray text for placeholder
    });

    it("shows correct styling for filter with value", () => {
      const value: DateRangeValue = {
        endDate: "2024-01-20T15:45:00Z",
        startDate: "2024-01-15T10:30:00Z",
      };

      render(
        <TestWrapper>
          <DateRangeFilter {...defaultProps} filter={{ ...mockFilter, value }} />
        </TestWrapper>
      );

      const displayValue = screen.getByText(/Jan 15, 2024/u);

      // Check that it has a different style class than placeholder text
      expect(displayValue).not.toHaveClass("css-1ij6rp6");
    });
  });

  describe("Translation Integration", () => {
    it("uses translation for placeholder text", () => {
      render(
        <TestWrapper>
          <DateRangeFilter {...defaultProps} />
        </TestWrapper>
      );

      expect(mockTranslate).toHaveBeenCalledWith("common:filters.selectDateRange");
      expect(screen.getByText("Select Date Range")).toBeInTheDocument();
    });

    it("uses translation for prefix text", () => {
      const value: DateRangeValue = {
        endDate: undefined,
        startDate: "2024-01-15T10:30:00Z",
      };

      render(
        <TestWrapper>
          <DateRangeFilter {...defaultProps} filter={{ ...mockFilter, value }} />
        </TestWrapper>
      );

      expect(mockTranslate).toHaveBeenCalledWith("common:filters.from");
      expect(screen.getByText(/From Jan 15, 2024/u)).toBeInTheDocument();
    });

    it("uses translation for error messages", async () => {
      render(
        <TestWrapper>
          <DateRangeFilter {...defaultProps} />
        </TestWrapper>
      );

      const [dateInput] = screen.getAllByPlaceholderText("YYYY/MM/DD");

      fireEvent.change(dateInput, { target: { value: "invalid-date" } });

      await waitFor(() => {
        expect(mockTranslate).toHaveBeenCalledWith("components:dateRangeFilter.validation.invalidDateFormat");
        expect(screen.getByText("Invalid date format.")).toBeInTheDocument();
      });
    });
  });

  describe("Input Validation", () => {
    it("shows error for invalid date format in start date", async () => {
      render(
        <TestWrapper>
          <DateRangeFilter {...defaultProps} />
        </TestWrapper>
      );

      const dateInputs = screen.getAllByPlaceholderText("YYYY/MM/DD");
      const startInput = dateInputs[0]; // First input is start date

      fireEvent.change(startInput, { target: { value: "invalid-date" } });

      await waitFor(() => {
        expect(screen.getByText("Invalid date format.")).toBeInTheDocument();
      });
    });

    it("shows error for invalid date format in end date", async () => {
      render(
        <TestWrapper>
          <DateRangeFilter {...defaultProps} />
        </TestWrapper>
      );

      const inputs = screen.getAllByPlaceholderText("YYYY/MM/DD");
      const endInput = inputs[1]; // Second input is end date

      fireEvent.change(endInput, { target: { value: "2024/13/50" } }); // Invalid month and day

      await waitFor(() => {
        expect(screen.getByText("Invalid date format.")).toBeInTheDocument();
      });
    });

    it("shows error for invalid time format in start time", async () => {
      render(
        <TestWrapper>
          <DateRangeFilter {...defaultProps} />
        </TestWrapper>
      );

      const startTimeInput = screen.getAllByPlaceholderText("HH:mm")[0];

      fireEvent.change(startTimeInput, { target: { value: "25:99" } }); // Invalid hour and minute

      await waitFor(() => {
        expect(screen.getByText("Invalid time format.")).toBeInTheDocument();
      });
    });

    it("shows error for invalid time format in end time", async () => {
      render(
        <TestWrapper>
          <DateRangeFilter {...defaultProps} />
        </TestWrapper>
      );

      const endTimeInput = screen.getAllByPlaceholderText("HH:mm")[1];

      fireEvent.change(endTimeInput, { target: { value: "abc:def" } }); // Invalid format

      await waitFor(() => {
        expect(screen.getByText("Invalid time format.")).toBeInTheDocument();
      });
    });

    it("shows error when start date/time is after end date/time", async () => {
      render(
        <TestWrapper>
          <DateRangeFilter {...defaultProps} />
        </TestWrapper>
      );

      const dateInputs = screen.getAllByPlaceholderText("YYYY/MM/DD");
      const timeInputs = screen.getAllByPlaceholderText("HH:mm");

      // Set start date to 2024/01/20 10:00
      fireEvent.change(dateInputs[0], { target: { value: "2024/01/20" } });
      fireEvent.change(timeInputs[0], { target: { value: "10:00" } });

      // Set end date to 2024/01/15 09:00 (before start)
      fireEvent.change(dateInputs[1], { target: { value: "2024/01/15" } });
      fireEvent.change(timeInputs[1], { target: { value: "09:00" } });

      await waitFor(() => {
        expect(screen.getByText("Start date/time must be before end date/time")).toBeInTheDocument();
      });
    });

    it("accepts valid date and time inputs", async () => {
      render(
        <TestWrapper>
          <DateRangeFilter {...defaultProps} />
        </TestWrapper>
      );

      const dateInputs = screen.getAllByPlaceholderText("YYYY/MM/DD");
      const timeInputs = screen.getAllByPlaceholderText("HH:mm");

      // Set valid start date/time
      fireEvent.change(dateInputs[0], { target: { value: "2024/01/15" } });
      fireEvent.change(timeInputs[0], { target: { value: "10:30" } });

      // Set valid end date/time
      fireEvent.change(dateInputs[1], { target: { value: "2024/01/20" } });
      fireEvent.change(timeInputs[1], { target: { value: "15:45" } });

      // No error messages should be present
      expect(screen.queryByText(/Invalid/)).not.toBeInTheDocument();
      expect(screen.queryByText(/must be before/)).not.toBeInTheDocument();
    });

    it("clears validation errors when invalid input is corrected", async () => {
      render(
        <TestWrapper>
          <DateRangeFilter {...defaultProps} />
        </TestWrapper>
      );

      const dateInputs = screen.getAllByPlaceholderText("YYYY/MM/DD");
      const startInput = dateInputs[0]; // First input is start date

      // Enter invalid date
      fireEvent.change(startInput, { target: { value: "invalid" } });

      await waitFor(() => {
        expect(screen.getByText("Invalid date format.")).toBeInTheDocument();
      });

      // Enter valid date
      fireEvent.change(startInput, { target: { value: "2024/01/15" } });

      await waitFor(() => {
        expect(screen.queryByText("Invalid date format.")).not.toBeInTheDocument();
      });
    });
  });
});
