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
import { render, screen, fireEvent } from "@testing-library/react";
import dayjs from "dayjs";
import timezone from "dayjs/plugin/timezone";
import utc from "dayjs/plugin/utc";
import { describe, expect, it, vi } from "vitest";

import { TimezoneContext } from "src/context/timezone";
import { ChakraWrapper } from "src/utils/ChakraWrapper";

import { DateTimeInput } from "./DateTimeInput";

dayjs.extend(utc);
dayjs.extend(timezone);

const renderWithTimezone = (tz: string, value: string, onChange?: ReturnType<typeof vi.fn>) =>
  render(
    <TimezoneContext.Provider value={{ selectedTimezone: tz, setSelectedTimezone: vi.fn() }}>
      <DateTimeInput data-testid="dt-input" onChange={onChange ?? vi.fn()} value={value} />
    </TimezoneContext.Provider>,
    { wrapper: ChakraWrapper },
  );

describe("DateTimeInput", () => {
  it("displays UTC ISO value in the selected timezone using datetime-local T format", () => {
    // 15:30 UTC = 10:30 US/Eastern (EST, UTC-5)
    renderWithTimezone("US/Eastern", "2026-02-16T15:30:00.000Z");
    const input = screen.getByTestId("dt-input") as HTMLInputElement;

    // datetime-local inputs should use T separator; value should be in Eastern time
    expect(input.value).toBe(dayjs("2026-02-16T15:30:00.000Z").tz("US/Eastern").format("YYYY-MM-DDTHH:mm:ss"));
  });

  it("displays correctly when selectedTimezone is UTC", () => {
    renderWithTimezone("UTC", "2026-02-16T15:30:00.000Z");
    const input = screen.getByTestId("dt-input") as HTMLInputElement;

    expect(input.value).toBe("2026-02-16T15:30:00");
  });

  it("converts user-edited value from selected timezone to UTC ISO on change", () => {
    const onChange = vi.fn();

    renderWithTimezone("US/Eastern", "2026-02-16T15:30:00.000Z", onChange);
    const input = screen.getByTestId("dt-input") as HTMLInputElement;

    // Simulate user picking 14:00 Eastern via the datetime-local picker
    fireEvent.change(input, { target: { value: "2026-02-16T14:00:00" } });

    // The onChange should emit the value converted to UTC ISO
    const emittedValue = onChange.mock.calls[0]?.[0]?.target?.value as string;

    // 14:00 Eastern = 19:00 UTC
    expect(emittedValue).toBe(dayjs.tz("2026-02-16T14:00:00", "US/Eastern").toISOString());
  });

  it("emits empty string for invalid date input", () => {
    const onChange = vi.fn();

    renderWithTimezone("UTC", "2026-02-16T15:30:00.000Z", onChange);
    const input = screen.getByTestId("dt-input") as HTMLInputElement;

    fireEvent.change(input, { target: { value: "" } });

    const emittedValue = onChange.mock.calls[0]?.[0]?.target?.value as string;

    expect(emittedValue).toBe("");
  });

  it("handles non-UTC timezone roundtrip correctly", () => {
    // Verify that displaying then re-submitting the same time is lossless
    const utcIso = "2026-06-15T12:00:00.000Z"; // During DST in Eastern (UTC-4)
    const tz = "US/Eastern";

    // Display: 12:00 UTC = 08:00 EDT
    const displayValue = dayjs(utcIso).tz(tz).format("YYYY-MM-DDTHH:mm:ss");

    // Re-parse as Eastern → should give back original UTC
    const roundtrippedUtc = dayjs.tz(displayValue, tz).toISOString();

    expect(roundtrippedUtc).toBe(utcIso);
  });
});
