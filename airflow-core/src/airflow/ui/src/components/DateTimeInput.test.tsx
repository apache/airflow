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
import { fireEvent, render, screen } from "@testing-library/react";
import dayjs from "dayjs";
import timezone from "dayjs/plugin/timezone";
import utc from "dayjs/plugin/utc";
import type { ChangeEvent } from "react";
import type { Mock } from "vitest";
import { describe, it, expect, vi } from "vitest";

import { TimezoneContext } from "src/context/timezone";
import { Wrapper } from "src/utils/Wrapper";

import { DateTimeInput } from "./DateTimeInput";

dayjs.extend(utc);
dayjs.extend(timezone);

type ChangeHandler = (event: ChangeEvent<HTMLInputElement>) => void;

const renderWithTimezone = (selectedTimezone: string) => {
  const onChange: Mock<ChangeHandler> = vi.fn();

  render(
    <TimezoneContext.Provider value={{ selectedTimezone, setSelectedTimezone: vi.fn() }}>
      <DateTimeInput onChange={onChange} value="" />
    </TimezoneContext.Provider>,
    { wrapper: Wrapper },
  );

  return { input: screen.getByTestId<HTMLInputElement>("datetime-input"), onChange };
};

const paste = (input: HTMLInputElement, text: string) =>
  fireEvent.paste(input, { clipboardData: { getData: () => text } });

const lastEmittedValue = (onChange: Mock<ChangeHandler>): string | undefined =>
  onChange.mock.calls.at(-1)?.[0].target.value;

describe("DateTimeInput onPaste timezone handling", () => {
  it("renders pasted UTC instant in the selected UTC timezone", () => {
    const { input, onChange } = renderWithTimezone("UTC");

    paste(input, "2026-01-15T10:30:00Z");

    expect(input.value).toBe("2026-01-15T10:30");
    expect(lastEmittedValue(onChange)).toBe("2026-01-15T10:30:00.000Z");
  });

  it("converts pasted UTC instant into a non-UTC selected timezone", () => {
    const { input, onChange } = renderWithTimezone("Asia/Seoul");

    paste(input, "2026-01-15T10:30:00Z");

    // 10:30 UTC == 19:30 Asia/Seoul (+09:00)
    expect(input.value).toBe("2026-01-15T19:30");
    expect(lastEmittedValue(onChange)).toBe("2026-01-15T10:30:00.000Z");
  });

  it("converts pasted offset value into the selected timezone", () => {
    const { input, onChange } = renderWithTimezone("UTC");

    paste(input, "2026-01-15T10:30:00+09:00");

    // 10:30 in +09:00 == 01:30 UTC
    expect(input.value).toBe("2026-01-15T01:30");
    expect(lastEmittedValue(onChange)).toBe("2026-01-15T01:30:00.000Z");
  });

  it("treats a bare datetime as being in the selected timezone", () => {
    const { input, onChange } = renderWithTimezone("Asia/Seoul");

    paste(input, "2026-01-15T10:30");

    // bare 10:30 interpreted as Asia/Seoul (+09:00) == 01:30 UTC
    expect(input.value).toBe("2026-01-15T10:30");
    expect(lastEmittedValue(onChange)).toBe("2026-01-15T01:30:00.000Z");
  });

  it("ignores invalid pasted strings", () => {
    const { input, onChange } = renderWithTimezone("UTC");

    paste(input, "not a date");

    expect(input.value).toBe("");
    expect(onChange).not.toHaveBeenCalled();
  });

  it("does not fire a pending debounced typing call after a paste", () => {
    vi.useFakeTimers();
    try {
      const { input, onChange } = renderWithTimezone("UTC");

      // 1. User types — schedules debouncedOnDateChange (1s delay).
      fireEvent.change(input, { target: { value: "2026-01-15T05:00" } });
      expect(onChange).not.toHaveBeenCalled();

      // 2. Within the debounce window, user pastes — fires onChange immediately.
      vi.advanceTimersByTime(300);
      paste(input, "2026-12-31T23:59:00Z");
      expect(onChange).toHaveBeenCalledTimes(1);

      // 3. Advance past the debounce delay. The pending typed call must NOT fire,
      // otherwise the parent form gets a redundant onChange (and any side effects
      // attached to it run twice).
      vi.advanceTimersByTime(2000);
      expect(onChange).toHaveBeenCalledTimes(1);
      expect(lastEmittedValue(onChange)).toBe("2026-12-31T23:59:00.000Z");
    } finally {
      vi.useRealTimers();
    }
  });
});
