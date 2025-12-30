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

describe("DateRangeFilter", () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  describe("Input Validation", () => {
  it("shows error for invalid date format in start date", async () => {
    render(
      <TestWrapper>
        <DateRangeFilter {...defaultProps} />
      </TestWrapper>,
    );

    const [startDateInput] = screen.getAllByPlaceholderText("YYYY/MM/DD");
      expect(startDateInput).toBeInTheDocument();

      fireEvent.change(startDateInput!, { target: { value: "invalid-date" } });

    await waitFor(() => {
        const errorMessage = screen.getByText("Invalid date format.");
        expect(errorMessage).toBeInTheDocument();
    });
  });

  it("shows error for invalid date format in end date", async () => {
    render(
      <TestWrapper>
        <DateRangeFilter {...defaultProps} />
      </TestWrapper>,
    );

    const [, endDateInput] = screen.getAllByPlaceholderText("YYYY/MM/DD");
      expect(endDateInput).toBeInTheDocument();

      fireEvent.change(endDateInput!, { target: { value: "invalid-date" } });

      await waitFor(() => {
        const errorMessage = screen.getByText("Invalid date format.");
        expect(errorMessage).toBeInTheDocument();
      });
    });

    it("rejects dates with invalid month (13th month)", async () => {
      render(
        <TestWrapper>
          <DateRangeFilter {...defaultProps} />
        </TestWrapper>,
      );

      const [startDateInput] = screen.getAllByPlaceholderText("YYYY/MM/DD");
      expect(startDateInput).toBeInTheDocument();
      fireEvent.change(startDateInput!, { target: { value: "2024/13/01" } });

      await waitFor(() => {
        expect(screen.getByText("Invalid date format.")).toBeInTheDocument();
      });
    });

    it("rejects dates with invalid day (32nd day)", async () => {
      render(
        <TestWrapper>
          <DateRangeFilter {...defaultProps} />
        </TestWrapper>,
      );

      const [startDateInput] = screen.getAllByPlaceholderText("YYYY/MM/DD");
      expect(startDateInput).toBeInTheDocument();
      fireEvent.change(startDateInput!, { target: { value: "2024/01/32" } });

      await waitFor(() => {
        expect(screen.getByText("Invalid date format.")).toBeInTheDocument();
      });
    });

    it("rejects dates with invalid day in February (30th)", async () => {
      render(
        <TestWrapper>
          <DateRangeFilter {...defaultProps} />
        </TestWrapper>,
      );

      const [startDateInput] = screen.getAllByPlaceholderText("YYYY/MM/DD");
      expect(startDateInput).toBeInTheDocument();
      fireEvent.change(startDateInput!, { target: { value: "2024/02/30" } });

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
      expect(startTimeInput).toBeInTheDocument();

      fireEvent.change(startTimeInput!, { target: { value: "invalid-time" } });

    await waitFor(() => {
        const errorMessage = screen.getByText("Invalid time format.");
        expect(errorMessage).toBeInTheDocument();
    });
  });

  it("shows error for invalid time format in end time", async () => {
    render(
      <TestWrapper>
        <DateRangeFilter {...defaultProps} />
      </TestWrapper>,
    );

    const [, endTimeInput] = screen.getAllByPlaceholderText("HH:mm");
      expect(endTimeInput).toBeInTheDocument();

      fireEvent.change(endTimeInput!, { target: { value: "invalid-time" } });

      await waitFor(() => {
        const errorMessage = screen.getByText("Invalid time format.");
        expect(errorMessage).toBeInTheDocument();
      });
    });

    it("rejects times with invalid hour (25:00)", async () => {
      render(
        <TestWrapper>
          <DateRangeFilter {...defaultProps} />
        </TestWrapper>,
      );

      const [startTimeInput] = screen.getAllByPlaceholderText("HH:mm");
      expect(startTimeInput).toBeInTheDocument();
      fireEvent.change(startTimeInput!, { target: { value: "25:00" } });

      await waitFor(() => {
        expect(screen.getByText("Invalid time format.")).toBeInTheDocument();
      });
    });

    it("rejects times with invalid minute (12:60)", async () => {
      render(
        <TestWrapper>
          <DateRangeFilter {...defaultProps} />
        </TestWrapper>,
      );

      const [startTimeInput] = screen.getAllByPlaceholderText("HH:mm");
      expect(startTimeInput).toBeInTheDocument();
      fireEvent.change(startTimeInput!, { target: { value: "12:60" } });

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

      expect(startDateInput).toBeInTheDocument();
      expect(endDateInput).toBeInTheDocument();
      expect(startTimeInput).toBeInTheDocument();
      expect(endTimeInput).toBeInTheDocument();

    // Set start date/time to be after end date/time
      fireEvent.change(startDateInput!, { target: { value: "2024/01/15" } });
      fireEvent.change(startTimeInput!, { target: { value: "10:00" } });
      fireEvent.change(endDateInput!, { target: { value: "2024/01/14" } });
      fireEvent.change(endTimeInput!, { target: { value: "09:00" } });

      await waitFor(() => {
        const errorMessage = screen.getByText("Start date/time must be before end date/time");
        expect(errorMessage).toBeInTheDocument();
      });
    });

    it("shows error when start time is after end time on the same day", async () => {
      render(
        <TestWrapper>
          <DateRangeFilter {...defaultProps} />
        </TestWrapper>,
      );

      const [startDateInput, endDateInput] = screen.getAllByPlaceholderText("YYYY/MM/DD");
      const [startTimeInput, endTimeInput] = screen.getAllByPlaceholderText("HH:mm");

      expect(startDateInput).toBeInTheDocument();
      expect(endDateInput).toBeInTheDocument();
      expect(startTimeInput).toBeInTheDocument();
      expect(endTimeInput).toBeInTheDocument();

      // Set same date but start time after end time
      fireEvent.change(startDateInput!, { target: { value: "2024/01/15" } });
      fireEvent.change(startTimeInput!, { target: { value: "15:00" } });
      fireEvent.change(endDateInput!, { target: { value: "2024/01/15" } });
      fireEvent.change(endTimeInput!, { target: { value: "10:00" } });

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

      expect(startDateInput).toBeInTheDocument();
      expect(endDateInput).toBeInTheDocument();
      expect(startTimeInput).toBeInTheDocument();
      expect(endTimeInput).toBeInTheDocument();

    // Set valid inputs
      fireEvent.change(startDateInput!, { target: { value: "2024/01/15" } });
      fireEvent.change(startTimeInput!, { target: { value: "09:00" } });
      fireEvent.change(endDateInput!, { target: { value: "2024/01/20" } });
      fireEvent.change(endTimeInput!, { target: { value: "17:00" } });

    await waitFor(() => {
      // Should not show any validation errors
      expect(screen.queryByText("Invalid date format.")).not.toBeInTheDocument();
      expect(screen.queryByText("Invalid time format.")).not.toBeInTheDocument();
      expect(screen.queryByText("Start date/time must be before end date/time")).not.toBeInTheDocument();
    });
  });

    it("accepts same date with start time before end time", async () => {
      render(
        <TestWrapper>
          <DateRangeFilter {...defaultProps} />
        </TestWrapper>,
      );

      const [startDateInput, endDateInput] = screen.getAllByPlaceholderText("YYYY/MM/DD");
      const [startTimeInput, endTimeInput] = screen.getAllByPlaceholderText("HH:mm");

      expect(startDateInput).toBeInTheDocument();
      expect(endDateInput).toBeInTheDocument();
      expect(startTimeInput).toBeInTheDocument();
      expect(endTimeInput).toBeInTheDocument();

      // Same date, valid time range
      fireEvent.change(startDateInput!, { target: { value: "2024/01/15" } });
      fireEvent.change(startTimeInput!, { target: { value: "09:00" } });
      fireEvent.change(endDateInput!, { target: { value: "2024/01/15" } });
      fireEvent.change(endTimeInput!, { target: { value: "17:00" } });

      await waitFor(() => {
        expect(screen.queryByText("Start date/time must be before end date/time")).not.toBeInTheDocument();
      });
    });

    it("accepts same date with same time (boundary case)", async () => {
      render(
        <TestWrapper>
          <DateRangeFilter {...defaultProps} />
        </TestWrapper>,
      );

      const [startDateInput, endDateInput] = screen.getAllByPlaceholderText("YYYY/MM/DD");
      const [startTimeInput, endTimeInput] = screen.getAllByPlaceholderText("HH:mm");

      expect(startDateInput).toBeInTheDocument();
      expect(endDateInput).toBeInTheDocument();
      expect(startTimeInput).toBeInTheDocument();
      expect(endTimeInput).toBeInTheDocument();

      // Same date and same time (should be valid per validateDateRange)
      fireEvent.change(startDateInput!, { target: { value: "2024/01/15" } });
      fireEvent.change(startTimeInput!, { target: { value: "12:00" } });
      fireEvent.change(endDateInput!, { target: { value: "2024/01/15" } });
      fireEvent.change(endTimeInput!, { target: { value: "12:00" } });

      await waitFor(() => {
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

      // First, set an invalid date
      fireEvent.change(startDateInput!, { target: { value: "invalid-date" } });

      await waitFor(() => {
        expect(screen.getByText("Invalid date format.")).toBeInTheDocument();
      });

      // Then, correct it to a valid date
      fireEvent.change(startDateInput!, { target: { value: "2024/01/15" } });

      await waitFor(() => {
        expect(screen.queryByText("Invalid date format.")).not.toBeInTheDocument();
      });
    });

    it("clears validation errors when invalid time is corrected", async () => {
      render(
        <TestWrapper>
          <DateRangeFilter {...defaultProps} />
        </TestWrapper>,
      );

      const [startTimeInput] = screen.getAllByPlaceholderText("HH:mm");

      // First, set an invalid time
      fireEvent.change(startTimeInput!, { target: { value: "invalid-time" } });

      await waitFor(() => {
        expect(screen.getByText("Invalid time format.")).toBeInTheDocument();
      });

      // Then, correct it to a valid time
      fireEvent.change(startTimeInput!, { target: { value: "09:00" } });

      await waitFor(() => {
        expect(screen.queryByText("Invalid time format.")).not.toBeInTheDocument();
      });
    });

    it("clears range validation error when dates are corrected", async () => {
      render(
        <TestWrapper>
          <DateRangeFilter {...defaultProps} />
        </TestWrapper>,
      );

      const [startDateInput, endDateInput] = screen.getAllByPlaceholderText("YYYY/MM/DD");
      const [startTimeInput, endTimeInput] = screen.getAllByPlaceholderText("HH:mm");

      expect(startDateInput).toBeInTheDocument();
      expect(endDateInput).toBeInTheDocument();
      expect(startTimeInput).toBeInTheDocument();
      expect(endTimeInput).toBeInTheDocument();

      fireEvent.change(startDateInput!, { target: { value: "2024/01/15" } });
      fireEvent.change(startTimeInput!, { target: { value: "10:00" } });
      fireEvent.change(endDateInput!, { target: { value: "2024/01/14" } });
      fireEvent.change(endTimeInput!, { target: { value: "09:00" } });

      await waitFor(() => {
        expect(screen.getByText("Start date/time must be before end date/time")).toBeInTheDocument();
      });

      // Correct the range
      fireEvent.change(endDateInput!, { target: { value: "2024/01/16" } });

      await waitFor(() => {
        expect(screen.queryByText("Start date/time must be before end date/time")).not.toBeInTheDocument();
      });
    });
  });

  describe("onChange Callback", () => {
    it("calls onChange with correct ISO string when valid date and time are entered", async () => {
      const mockOnChange = vi.fn();
      const props = { ...defaultProps, onChange: mockOnChange };

      render(
        <TestWrapper>
          <DateRangeFilter {...props} />
        </TestWrapper>,
      );

      const [startDateInput, endDateInput] = screen.getAllByPlaceholderText("YYYY/MM/DD");
      const [startTimeInput, endTimeInput] = screen.getAllByPlaceholderText("HH:mm");

      expect(startDateInput).toBeInTheDocument();
      expect(endDateInput).toBeInTheDocument();
      expect(startTimeInput).toBeInTheDocument();
      expect(endTimeInput).toBeInTheDocument();

      // Set start date and time
      fireEvent.change(startDateInput!, { target: { value: "2024/01/15" } });
      fireEvent.change(startTimeInput!, { target: { value: "09:00" } });

      // Wait for start date onChange call
      await waitFor(() => {
        expect(mockOnChange).toHaveBeenCalled();
      });

      // Verify start date call has ISO string format
      const startDateCall = mockOnChange.mock.calls.find((call) => call[0]?.startDate);
      expect(startDateCall).toBeDefined();
      expect(startDateCall![0].startDate).toMatch(/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}/);

      // Set end date and time
      fireEvent.change(endDateInput!, { target: { value: "2024/01/20" } });
      fireEvent.change(endTimeInput!, { target: { value: "17:00" } });

      // Wait for end date onChange call
      await waitFor(() => {
        expect(mockOnChange.mock.calls.length).toBeGreaterThan(1);
      });

      // Verify end date call has ISO string format
      const endDateCall = mockOnChange.mock.calls.find((call) => call[0]?.endDate);
      expect(endDateCall).toBeDefined();
      expect(endDateCall![0].endDate).toMatch(/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}/);
    });

    it("calls onChange when only start date is provided", async () => {
      const mockOnChange = vi.fn();
      const props = { ...defaultProps, onChange: mockOnChange };

      render(
        <TestWrapper>
          <DateRangeFilter {...props} />
        </TestWrapper>,
      );

      const [startDateInput] = screen.getAllByPlaceholderText("YYYY/MM/DD");
      const [startTimeInput] = screen.getAllByPlaceholderText("HH:mm");

      expect(startDateInput).toBeInTheDocument();
      expect(startTimeInput).toBeInTheDocument();

      fireEvent.change(startDateInput!, { target: { value: "2024/01/15" } });
      fireEvent.change(startTimeInput!, { target: { value: "09:00" } });

      await waitFor(() => {
        expect(mockOnChange).toHaveBeenCalled();
      });

      expect(mockOnChange.mock.calls.length).toBeGreaterThan(0);
      const lastCall = mockOnChange.mock.calls[mockOnChange.mock.calls.length - 1]!;
      expect(lastCall[0]).toHaveProperty("startDate");
      expect(lastCall[0].startDate).toBeDefined();
    });

    it("calls onChange when only end date is provided", async () => {
      const mockOnChange = vi.fn();
      const props = { ...defaultProps, onChange: mockOnChange };

      render(
        <TestWrapper>
          <DateRangeFilter {...props} />
        </TestWrapper>,
      );

      const [, endDateInput] = screen.getAllByPlaceholderText("YYYY/MM/DD");
      const [, endTimeInput] = screen.getAllByPlaceholderText("HH:mm");

      expect(endDateInput).toBeInTheDocument();
      expect(endTimeInput).toBeInTheDocument();

      fireEvent.change(endDateInput!, { target: { value: "2024/01/20" } });
      fireEvent.change(endTimeInput!, { target: { value: "17:00" } });

      await waitFor(() => {
        expect(mockOnChange).toHaveBeenCalled();
      });

      expect(mockOnChange.mock.calls.length).toBeGreaterThan(0);
      const lastCall = mockOnChange.mock.calls[mockOnChange.mock.calls.length - 1]!;
      expect(lastCall[0]).toHaveProperty("endDate");
      expect(lastCall[0].endDate).toBeDefined();
    });

    it("does not call onChange when date format is invalid", async () => {
      const mockOnChange = vi.fn();
      const props = { ...defaultProps, onChange: mockOnChange };

      render(
        <TestWrapper>
          <DateRangeFilter {...props} />
        </TestWrapper>,
      );

      const [startDateInput] = screen.getAllByPlaceholderText("YYYY/MM/DD");
      expect(startDateInput).toBeInTheDocument();

      fireEvent.change(startDateInput!, { target: { value: "invalid-date" } });

      await waitFor(() => {
        expect(screen.getByText("Invalid date format.")).toBeInTheDocument();
      });

      expect(mockOnChange).not.toHaveBeenCalled();
    });
  });

  describe("onRemove Callback", () => {
    it("calls onRemove when remove button is clicked", () => {
      const mockOnRemove = vi.fn();
      const props = {
        ...defaultProps,
        filter: { ...mockFilter, value: { startDate: "2024-01-15T09:00:00Z", endDate: "2024-01-20T17:00:00Z" } },
        onRemove: mockOnRemove,
      };

      const { container } = render(
        <TestWrapper>
          <DateRangeFilter {...props} />
        </TestWrapper>,
      );

      const allSvgs = container.querySelectorAll("svg");
      const closeIcon = Array.from(allSvgs).find((svg) => {
        const path = svg.querySelector('path[d*="M19 6.41"]');
        return path !== null;
      });

      expect(closeIcon).toBeInTheDocument();

      const removeButton = closeIcon?.parentElement as HTMLElement | null;
      expect(removeButton).toBeInTheDocument();

      fireEvent.click(removeButton!, { bubbles: true });

      expect(mockOnRemove).toHaveBeenCalledTimes(1);
    });
  });

  describe("Display Value Formatting", () => {
    it("displays placeholder when no value is set", () => {
      render(
        <TestWrapper>
          <DateRangeFilter {...defaultProps} />
        </TestWrapper>,
      );

      expect(screen.getByText("Select Date Range")).toBeInTheDocument();
    });

    it("displays formatted date range when both dates are set", () => {
      const props = {
        ...defaultProps,
        filter: { ...mockFilter, value: { startDate: "2024-01-15T09:00:00Z", endDate: "2024-01-20T17:00:00Z" } },
      };

      render(
        <TestWrapper>
          <DateRangeFilter {...props} />
        </TestWrapper>,
      );

      const displayText = screen.getByText(/Jan 15, 2024/);
      expect(displayText).toBeInTheDocument();
    });

    it("displays 'From' prefix when only start date is set", () => {
      const props = {
        ...defaultProps,
        filter: { ...mockFilter, value: { startDate: "2024-01-15T09:00:00Z", endDate: undefined } },
      };

      render(
        <TestWrapper>
          <DateRangeFilter {...props} />
        </TestWrapper>,
      );

      expect(screen.getByText(/From/)).toBeInTheDocument();
    });

    it("displays 'To' prefix when only end date is set", () => {
      const props = {
        ...defaultProps,
        filter: { ...mockFilter, value: { startDate: undefined, endDate: "2024-01-20T17:00:00Z" } },
      };

      render(
        <TestWrapper>
          <DateRangeFilter {...props} />
        </TestWrapper>,
      );

      expect(screen.getByText(/To/)).toBeInTheDocument();
    });
  });

  describe("Edge Cases", () => {
    it("handles leap year dates correctly", async () => {
      render(
        <TestWrapper>
          <DateRangeFilter {...defaultProps} />
        </TestWrapper>,
      );

      const [startDateInput] = screen.getAllByPlaceholderText("YYYY/MM/DD");
      expect(startDateInput).toBeInTheDocument();

      // February 29 in a leap year
      fireEvent.change(startDateInput!, { target: { value: "2024/02/29" } });

    await waitFor(() => {
      expect(screen.queryByText("Invalid date format.")).not.toBeInTheDocument();
      });
    });

    it("rejects February 29 in non-leap year", async () => {
      render(
        <TestWrapper>
          <DateRangeFilter {...defaultProps} />
        </TestWrapper>,
      );

      const [startDateInput] = screen.getAllByPlaceholderText("YYYY/MM/DD");
      expect(startDateInput).toBeInTheDocument();

      // February 29 in a non-leap year
      fireEvent.change(startDateInput!, { target: { value: "2023/02/29" } });

      await waitFor(() => {
        expect(screen.getByText("Invalid date format.")).toBeInTheDocument();
      });
    });

    it("handles month boundary dates correctly", async () => {
      render(
        <TestWrapper>
          <DateRangeFilter {...defaultProps} />
        </TestWrapper>,
      );

      const [startDateInput, endDateInput] = screen.getAllByPlaceholderText("YYYY/MM/DD");
      expect(startDateInput).toBeInTheDocument();
      expect(endDateInput).toBeInTheDocument();

      fireEvent.change(startDateInput!, { target: { value: "2024/01/31" } });
      fireEvent.change(endDateInput!, { target: { value: "2024/02/01" } });

      await waitFor(() => {
        expect(screen.queryByText("Invalid date format.")).not.toBeInTheDocument();
        expect(screen.queryByText("Start date/time must be before end date/time")).not.toBeInTheDocument();
      });
    });

    it("handles year boundary dates correctly", async () => {
      render(
        <TestWrapper>
          <DateRangeFilter {...defaultProps} />
        </TestWrapper>,
      );

      const [startDateInput, endDateInput] = screen.getAllByPlaceholderText("YYYY/MM/DD");
      expect(startDateInput).toBeInTheDocument();
      expect(endDateInput).toBeInTheDocument();

      fireEvent.change(startDateInput!, { target: { value: "2023/12/31" } });
      fireEvent.change(endDateInput!, { target: { value: "2024/01/01" } });

      await waitFor(() => {
        expect(screen.queryByText("Invalid date format.")).not.toBeInTheDocument();
        expect(screen.queryByText("Start date/time must be before end date/time")).not.toBeInTheDocument();
      });
    });

    it("handles empty time input (defaults to 00:00)", async () => {
      const mockOnChange = vi.fn();
      const props = { ...defaultProps, onChange: mockOnChange };

      render(
        <TestWrapper>
          <DateRangeFilter {...props} />
        </TestWrapper>,
      );

      const [startDateInput] = screen.getAllByPlaceholderText("YYYY/MM/DD");
      expect(startDateInput).toBeInTheDocument();

      fireEvent.change(startDateInput!, { target: { value: "2024/01/15" } });

      await waitFor(() => {
        expect(mockOnChange).toHaveBeenCalled();
      });

      expect(mockOnChange.mock.calls.length).toBeGreaterThan(0);
      const lastCall = mockOnChange.mock.calls[mockOnChange.mock.calls.length - 1]!;
      expect(lastCall[0].startDate).toBeDefined();
      expect(lastCall[0].startDate).toMatch(/T00:00:00/);
    });

    it("handles null filter value gracefully", () => {
      const props = {
        ...defaultProps,
        filter: { ...mockFilter, value: null },
      };

      expect(() => {
        render(
          <TestWrapper>
            <DateRangeFilter {...props} />
          </TestWrapper>,
        );
      }).not.toThrow();
    });

    it("handles undefined filter value gracefully", () => {
      const props = {
        ...defaultProps,
        filter: { ...mockFilter, value: undefined },
      };

      expect(() => {
        render(
          <TestWrapper>
            <DateRangeFilter {...props} />
          </TestWrapper>,
        );
      }).not.toThrow();
    });
  });
});
