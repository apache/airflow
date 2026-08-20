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

// The date + time inputs live inside a popover, so the trigger has to be opened before they exist.
const openPicker = async (selectedTimezone: string, props?: { endOfDay?: boolean; value?: string }) => {
  const onChange: Mock<ChangeHandler> = vi.fn();

  render(
    <TimezoneContext.Provider value={{ selectedTimezone, setSelectedTimezone: vi.fn() }}>
      <DateTimeInput onChange={onChange} value="" {...props} />
    </TimezoneContext.Provider>,
    { wrapper: Wrapper },
  );

  fireEvent.click(screen.getByTestId("datetime-input"));

  return {
    dateInput: await screen.findByPlaceholderText<HTMLInputElement>("YYYY/MM/DD"),
    onChange,
    timeInput: screen.getByPlaceholderText<HTMLInputElement>("HH:mm"),
  };
};

const lastEmittedValue = (onChange: Mock<ChangeHandler>): string | undefined =>
  onChange.mock.calls.at(-1)?.[0].target.value;

const type = (input: HTMLInputElement, value: string) => fireEvent.change(input, { target: { value } });

describe("DateTimeInput", () => {
  it("emits the start of the day when only a date is entered", async () => {
    const { dateInput, onChange } = await openPicker("UTC");

    type(dateInput, "2026/01/15");

    expect(lastEmittedValue(onChange)).toBe("2026-01-15T00:00:00.000Z");
  });

  it("emits the end of the day when only a date is entered and endOfDay is set", async () => {
    const { dateInput, onChange } = await openPicker("UTC", { endOfDay: true });

    type(dateInput, "2026/01/15");

    expect(lastEmittedValue(onChange)).toBe("2026-01-15T23:59:59.999Z");
  });

  it("combines the entered date and time", async () => {
    const { dateInput, onChange, timeInput } = await openPicker("UTC");

    type(dateInput, "2026/01/15");
    type(timeInput, "10:30");

    expect(lastEmittedValue(onChange)).toBe("2026-01-15T10:30:00.000Z");
  });

  it("interprets the entered wall-clock time in the selected timezone", async () => {
    const { dateInput, onChange, timeInput } = await openPicker("Asia/Seoul");

    type(dateInput, "2026/01/15");
    type(timeInput, "10:30");

    // 10:30 in Asia/Seoul (+09:00) == 01:30 UTC
    expect(lastEmittedValue(onChange)).toBe("2026-01-15T01:30:00.000Z");
  });

  it("splits an incoming value into the date and time fields in the selected timezone", async () => {
    const { dateInput, timeInput } = await openPicker("Asia/Seoul", { value: "2026-01-15T10:30:00Z" });

    // 10:30 UTC == 19:30 Asia/Seoul
    expect(dateInput.value).toBe("2026/01/15");
    expect(timeInput.value).toBe("19:30");
  });

  it("emits an empty value when the date is cleared", async () => {
    const { dateInput, onChange } = await openPicker("UTC", { value: "2026-01-15T10:30:00Z" });

    type(dateInput, "");

    expect(lastEmittedValue(onChange)).toBe("");
  });

  it("does not emit while the date is an invalid format", async () => {
    const { dateInput, onChange } = await openPicker("UTC");

    type(dateInput, "2026/99/99");

    expect(onChange).not.toHaveBeenCalled();
  });
});
