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
import type { ReactNode } from "react";
import { useState } from "react";

import "@testing-library/jest-dom/vitest";
import { render, screen, fireEvent, waitFor, cleanup } from "@testing-library/react";
import dayjs from "dayjs";
import timezone from "dayjs/plugin/timezone";
import utc from "dayjs/plugin/utc";
import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";

import { TimezoneContext } from "src/context/timezone";
import { ChakraWrapper } from "src/utils/ChakraWrapper";

import type { DateRangeValue, FilterPluginProps } from "../types";
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

// A UI timezone whose offset differs from the runner's local offset at the
// instant used below, so tests exercise a real browser/UI timezone mismatch no
// matter which timezone CI runs in (CI runners are usually UTC).
const mismatchedTimezone = (instant: string): string => {
  const runnerOffset = dayjs(instant).utcOffset();
  const candidates = ["UTC", "Asia/Kolkata", "America/New_York", "Asia/Seoul"];

  return candidates.find((tz) => dayjs.tz(instant, tz).utcOffset() !== runnerOffset) ?? "UTC";
};

const FILTERED_INSTANT = "2024-01-15T01:00:00.000Z";
const uiTimezone = mismatchedTimezone(FILTERED_INSTANT);

const TestWrapper = ({
  children,
  selectedTimezone = "UTC",
}: {
  readonly children: ReactNode;
  readonly selectedTimezone?: string;
}) => {
  const timezoneContextValue = {
    availableTimezones: ["UTC", "America/New_York"],
    selectedTimezone,
    setSelectedTimezone: vi.fn(),
  };

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

const pressEnter = (input: HTMLElement | undefined) => {
  if (input) {
    fireEvent.keyDown(input, { key: "Enter" });
  }
};

const focusInput = (input: HTMLElement | undefined) => {
  if (input) {
    fireEvent.focus(input);
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

const renderFilter = (props: FilterPluginProps = defaultProps, selectedTimezone = "UTC") =>
  render(
    <TestWrapper selectedTimezone={selectedTimezone}>
      <DateRangeFilter {...props} />
    </TestWrapper>,
  );

// The popover content mounts asynchronously after the trigger is clicked.
const openPicker = async () => {
  fireEvent.click(screen.getByTestId("dateRange-pill"));
  fireEvent.click(screen.getByText("Date Range:"));
  await waitFor(() => {
    expect(screen.getAllByPlaceholderText("YYYY/MM/DD").length).toBe(2);
  });
};

const closePicker = async () => {
  fireEvent.click(screen.getByText("Date Range:"));
  // Wait for the dismissal to fully settle (commit + collapse back to the pill)
  // so any commit triggered by closing has landed before asserting on it.
  await waitFor(() => {
    expect(screen.getByTestId("dateRange-pill")).toBeInTheDocument();
  });
};

describe("DateRangeFilter", () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  afterEach(() => {
    cleanup();
  });

  it("includes the whole end date when its time is empty", async () => {
    const onChange = vi.fn();

    renderFilter({ ...defaultProps, onChange });
    const { endDateInput } = getInputs();

    changeDateInput(endDateInput, "2024/01/15");
    expect(onChange).not.toHaveBeenCalled();

    pressEnter(endDateInput);

    await waitFor(() => {
      expect(onChange).toHaveBeenLastCalledWith({
        endDate: "2024-01-15T23:59:59.999Z",
        startDate: undefined,
      });
    });
  });

  it("keeps typed input local and does not commit on every keystroke", () => {
    const onChange = vi.fn();

    renderFilter({ ...defaultProps, onChange });
    const { startDateInput } = getInputs();

    // Committing per keystroke used to sync the URL search params, whose
    // value was then written back into the inputs mid-typing and made the
    // picker unusable.
    changeDateInput(startDateInput, "2024");
    changeDateInput(startDateInput, "2024/01");
    changeDateInput(startDateInput, "2024/01/1");
    changeDateInput(startDateInput, "2024/01/15");

    expect(onChange).not.toHaveBeenCalled();
    expect(startDateInput).toHaveValue("2024/01/15");
  });

  it("does not shift the value when opening and closing the picker without edits", async () => {
    const onChange = vi.fn();
    const props = {
      ...defaultProps,
      filter: { ...mockFilter, value: { endDate: undefined, startDate: FILTERED_INSTANT } },
      onChange,
    };

    renderFilter(props, uiTimezone);
    await openPicker();

    const { startDateInput, startTimeInput } = getInputs();
    const expected = dayjs(FILTERED_INSTANT).tz(uiTimezone);

    // The inputs are labeled with and committed in the selected timezone, so
    // they must be filled in that timezone too. Filling them in the browser
    // timezone made closing the picker re-commit the displayed wall time as if
    // it were in the selected timezone, shifting the value.
    expect(startDateInput).toHaveValue(expected.format("YYYY/MM/DD"));
    expect(startTimeInput).toHaveValue(expected.format("HH:mm"));

    await closePicker();

    expect(onChange).not.toHaveBeenCalled();
  });

  it("commits a clicked calendar date in the selected timezone", async () => {
    const onChange = vi.fn();
    const props = {
      ...defaultProps,
      filter: { ...mockFilter, value: { endDate: undefined, startDate: FILTERED_INSTANT } },
      onChange,
    };

    renderFilter(props, uiTimezone);
    await openPicker();

    // Opening the picker focuses the start input, so aim the next pick at the
    // range's end explicitly.
    focusInput(getInputs().endDateInput);
    fireEvent.click(screen.getByText("16"));

    expect(onChange).toHaveBeenCalledTimes(1);
    expect(onChange).toHaveBeenLastCalledWith({
      endDate: dayjs.tz("2024-01-16", uiTimezone).endOf("day").toISOString(),
      startDate: FILTERED_INSTANT,
    });
  });

  it("does not re-commit a parent-synced range when the picker closes", async () => {
    const onChange = vi.fn();
    const StatefulFilter = () => {
      const [value, setValue] = useState<DateRangeValue>({ endDate: undefined, startDate: FILTERED_INSTANT });

      return (
        <DateRangeFilter
          {...defaultProps}
          filter={{ ...mockFilter, value }}
          onChange={(next) => {
            onChange(next);
            setValue(next as DateRangeValue);
          }}
        />
      );
    };

    render(
      <TestWrapper selectedTimezone={uiTimezone}>
        <StatefulFilter />
      </TestWrapper>,
    );

    await openPicker();
    fireEvent.click(screen.getByText("16"));
    await closePicker();

    // The parent-synced end-of-day 23:59:59.999 marker is displayed as 23:59 in
    // the minute-granular inputs; re-deriving it on close must be a no-op.
    expect(onChange).toHaveBeenCalledTimes(1);
  });

  it("accepts a start time on the end date when the end time is empty", async () => {
    renderFilter();
    const { endDateInput, startDateInput, startTimeInput } = getInputs();

    changeDateInput(startDateInput, "2024/01/15");
    changeTimeInput(startTimeInput, "10:00");
    changeDateInput(endDateInput, "2024/01/15");

    await waitForNoError("Start date/time must be before end date/time");
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
