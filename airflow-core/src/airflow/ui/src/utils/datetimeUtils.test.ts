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
import dayjs from "dayjs";
import dayjsDuration from "dayjs/plugin/duration";
import { describe, it, expect, vi, beforeAll, afterAll } from "vitest";

import {
  getDuration,
  getDurationTickStep,
  getElapsedSeconds,
  humanizeSeconds,
  renderDuration,
  renderExactDuration,
  getRelativeTime,
} from "./datetimeUtils";

dayjs.extend(dayjsDuration);

// CLDR's own strings shift between ICU releases — de narrow "1 Std." became "1h" in ICU 78 — and the
// runtime ICU differs across CI, contributor machines and browsers. So localized cases assert the
// composition we control (which units, what precision, which style, joined in order) and leave the
// wording to the platform. Only the English cases pin literals, as those encode our band policy.
const expectDuration = (
  locale: string,
  style: "long" | "narrow",
  parts: Array<[Intl.NumberFormatOptions["unit"], number, number?]>,
) => {
  const formatted = parts.map(([unit, value, fractionDigits = 0]) =>
    new Intl.NumberFormat(locale, {
      maximumFractionDigits: fractionDigits,
      style: "unit",
      unit,
      unitDisplay: style,
    }).format(value),
  );

  return formatted.length > 1
    ? new Intl.ListFormat(locale, { style, type: "unit" }).format(formatted)
    : formatted[0];
};

describe("renderDuration", () => {
  it.each([
    [0, "0s"],
    [0.0000004, "<1ms"],
    [0.0009, "<1ms"],
    [0.001, "1ms"],
    [0.083, "83ms"],
    [0.9994, "999ms"],
    // Rounding up out of the millisecond band must promote to seconds, not print "1000ms".
    [0.9996, "1s"],
    [1, "1s"],
    [1.5, "1.5s"],
    [9.87456, "9.87s"],
    // Three significant digits means one decimal from 10s up, two below it.
    [14.846, "14.8s"],
    [15, "15s"],
    [45, "45s"],
    [59.9, "59.9s"],
    // Rounding at the band's precision spills into the next band.
    [59.96, "1m"],
    [60, "1m"],
    [65.25, "1m 5s"],
    [545, "9m 5s"],
    [540, "9m"],
    [3599.6, "1h"],
    [3600, "1h"],
    [3725.4, "1h 2m"],
    [5400, "1h 30m"],
    [86_399.6, "1d"],
    [86_400, "1d"],
    [90_061.2, "1d 1h"],
    // Rounds rather than truncates: 1d 4h 30m is nearer 1d 5h.
    [102_600, "1d 5h"],
    [281_445, "3d 6h"],
  ])("formats %s seconds as %s", (seconds, expected) => {
    expect(renderDuration(seconds, "en")).toBe(expected);
  });

  it.each([[null], [undefined], [Number.NaN], [Number.POSITIVE_INFINITY], [-5]])(
    "returns undefined without a usable duration (%s)",
    (seconds) => {
      expect(renderDuration(seconds, "en")).toBeUndefined();
    },
  );

  it("accepts dayjs durations as well as numbers", () => {
    expect(renderDuration(dayjs.duration(10, "seconds"), "en")).toBe("10s");
    expect(renderDuration(dayjs.duration(0.083, "seconds"), "en")).toBe("83ms");
    expect(renderDuration(dayjs.duration(3725.4, "seconds"), "en")).toBe("1h 2m");
  });

  it.each([
    ["de", 0.083, [["millisecond", 83]]],
    ["de", 14.846, [["second", 14.8, 1]]],
    [
      "de",
      3725.4,
      [
        ["hour", 1],
        ["minute", 2],
      ],
    ],
    [
      "fr",
      281_445,
      [
        ["day", 3],
        ["hour", 6],
      ],
    ],
    [
      "ru",
      3725.4,
      [
        ["hour", 1],
        ["minute", 2],
      ],
    ],
    [
      "ja",
      65.25,
      [
        ["minute", 1],
        ["second", 5],
      ],
    ],
    [
      "ar",
      3725.4,
      [
        ["hour", 1],
        ["minute", 2],
      ],
    ],
    [
      "pl",
      545,
      [
        ["minute", 9],
        ["second", 5],
      ],
    ],
    [
      "zh-CN",
      545,
      [
        ["minute", 9],
        ["second", 5],
      ],
    ],
    ["pt", 604_800, [["day", 7]]],
    ["it", 604_800, [["day", 7]]],
  ] as Array<[string, number, Array<[Intl.NumberFormatOptions["unit"], number, number?]>]>)(
    "localizes %s duration of %s seconds",
    (locale, seconds, parts) => {
      expect(renderDuration(seconds, locale)).toBe(expectDuration(locale, "narrow", parts));
    },
  );

  // Properties CLDR has held stable for decades, unlike the unit abbreviations themselves.
  it("uses the locale's decimal separator and script", () => {
    expect(renderDuration(14.846, "fr")).toContain("14,8");
    expect(renderDuration(14.846, "en")).toContain("14.8");
    expect(renderDuration(3725.4, "ru")).toMatch(/\p{Script=Cyrillic}/u);
    expect(renderDuration(3725.4, "de")).toBe(
      expectDuration("de", "narrow", [
        ["hour", 1],
        ["minute", 2],
      ]),
    );
  });

  it.each([["en"], ["de"], ["ru"]])("marks sub-millisecond durations as under 1ms in %s", (locale) => {
    expect(renderDuration(0.0004, locale)).toBe(`<${expectDuration(locale, "narrow", [["millisecond", 1]])}`);
  });

  it("falls back to English rather than throwing on a language Intl rejects", () => {
    expect(renderDuration(3725.4, "not a locale!")).toBe("1h 2m");
  });
});

describe("getDuration", () => {
  it.each([
    ["2024-03-14T10:00:00.000Z", "2024-03-14T10:00:00.083Z", "83ms"],
    ["2024-03-14T10:00:00.000Z", "2024-03-14T10:00:05.5111111Z", "5.51s"],
    ["2024-03-14T10:00:00.000Z", "2024-03-14T10:00:14.846Z", "14.8s"],
    ["2024-03-14T10:00:00.000Z", "2024-03-14T12:30:00.000Z", "2h 30m"],
    ["2024-03-14T10:00:00.000Z", "2024-03-15T10:00:00.000Z", "1d"],
    ["2024-03-14T10:00:00.000Z", "2024-03-17T15:30:45.000Z", "3d 6h"],
  ])("renders %s to %s as %s", (start, end, expected) => {
    expect(getDuration(start, end, "en")).toBe(expected);
  });

  it("forwards the locale to the formatter", () => {
    expect(getDuration("2024-03-14T10:00:00.000Z", "2024-03-14T12:30:00.000Z", "de")).toBe(
      expectDuration("de", "narrow", [
        ["hour", 2],
        ["minute", 30],
      ]),
    );
  });

  it("handles null or undefined values", () => {
    expect(getDuration(null, null)).toBe(undefined);
    expect(getDuration(undefined, undefined)).toBe(undefined);
    expect(getDuration(null, "2024-03-14T10:00:10.000Z")).toBe(undefined);
  });

  it("falls back to current time when endDate is null (running task)", () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2024-03-14T10:00:10.000Z"));

    const start = "2024-03-14T10:00:00.000Z";

    expect(getDuration(start, null, "en")).toBe("10s");
    expect(getDuration(start, undefined, "en")).toBe("10s");

    vi.useRealTimers();
  });
});

describe("renderExactDuration", () => {
  // renderDuration rounds to two units, so "1h 2m" spans a full minute. The exact form backs the
  // `title` on the duration columns, where two similar runs have to be told apart.
  it.each([
    [3725.412, "1h 2m 5.412s"],
    [102_600, "1d 4h 30m"],
    [281_445, "3d 6h 10m 45s"],
    [3600, "1h"],
    [45.5, "45.5s"],
    [0.083, "83ms"],
  ])("renders %s seconds in full as %s", (seconds, expected) => {
    expect(renderExactDuration(seconds, "en")).toBe(expected);
  });

  it("keeps precision the rounded form drops", () => {
    expect(renderDuration(3725.412, "en")).toBe("1h 2m");
    expect(renderExactDuration(3725.412, "en")).toBe("1h 2m 5.412s");
  });

  it.each([[null], [undefined], [Number.NaN], [-5]])("returns undefined for %s", (seconds) => {
    expect(renderExactDuration(seconds, "en")).toBeUndefined();
  });

  it("localizes like the rounded form", () => {
    expect(renderExactDuration(3725.412, "de")).toBe(
      expectDuration("de", "narrow", [
        ["hour", 1],
        ["minute", 2],
        ["second", 5.412, 3],
      ]),
    );
  });
});

describe("getElapsedSeconds", () => {
  it.each([
    ["2024-03-14T10:00:00.000Z", "2024-03-14T10:00:14.846Z", 14.846],
    ["2024-03-14T10:00:00.000Z", "2024-03-14T12:30:00.000Z", 9000],
  ])("measures %s to %s as %s seconds", (start, end, expected) => {
    expect(getElapsedSeconds(start, end)).toBe(expected);
  });

  it.each([[null], [undefined], ["not a date"]])("returns undefined without a usable start (%s)", (start) => {
    expect(getElapsedSeconds(start, "2024-03-14T10:00:10.000Z")).toBeUndefined();
  });

  it("returns undefined when the end date is unparsable", () => {
    expect(getElapsedSeconds("2024-03-14T10:00:00.000Z", "not a date")).toBeUndefined();
  });

  it("measures against now when the end date is absent (running task)", () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2024-03-14T10:00:10.000Z"));

    expect(getElapsedSeconds("2024-03-14T10:00:00.000Z", null)).toBe(10);

    vi.useRealTimers();
  });
});

describe("getRelativeTime", () => {
  const fixedNow = new Date("2024-03-14T10:00:10.000Z");

  beforeAll(() => {
    vi.useFakeTimers();
    vi.setSystemTime(fixedNow);
  });

  afterAll(() => {
    vi.useRealTimers();
  });

  it.each([
    ["2024-03-14T10:00:00.000Z", "10 seconds ago"],
    ["2024-03-14T10:00:20.000Z", "in 10 seconds"],
    // The largest unit the gap reaches wins, rather than "2700 seconds ago".
    ["2024-03-14T09:15:10.000Z", "45 minutes ago"],
    ["2024-03-14T07:00:10.000Z", "3 hours ago"],
    ["2024-03-11T10:00:10.000Z", "3 days ago"],
    ["2024-02-14T10:00:10.000Z", "4 weeks ago"],
    ["2024-01-14T10:00:10.000Z", "2 months ago"],
    ["2022-03-14T10:00:10.000Z", "2 years ago"],
  ])("describes %s as %s", (date, expected) => {
    expect(getRelativeTime(date, "en")).toBe(expected);
  });

  // Rounding used to saturate the unit the magnitude was picked from, printing the unit's own
  // ceiling instead of promoting: "60 minutes ago" where dayjs's fromNow said "an hour ago".
  it.each([
    // 59.7 minutes: rounds to 60, which used to print "60 minutes ago".
    ["2024-03-14T09:00:28.000Z", "1 hour ago"],
    // 23.9 hours -> "24 hours ago" before.
    ["2024-03-13T10:06:50.000Z", "yesterday"],
    // 365.2 days -> "12 months ago" before.
    ["2023-03-15T04:10:10.000Z", "last year"],
  ])("promotes %s to the next unit up rather than saturating", (date, expected) => {
    expect(getRelativeTime(date, "en")).toBe(expected);
  });

  it.each([["de"], ["fr"], ["ru"], ["ja"]])("localizes relative time for %s", (locale) => {
    expect(getRelativeTime("2024-03-14T10:00:00.000Z", locale)).toBe(
      new Intl.RelativeTimeFormat(locale, { numeric: "auto" }).format(-10, "second"),
    );
    expect(getRelativeTime("2024-03-14T10:00:00.000Z", locale)).not.toBe(
      getRelativeTime("2024-03-14T10:00:00.000Z", "en"),
    );
  });

  it.each([[undefined], [null], [""], ["not a date"]])(
    "returns an empty string without a usable date (%s)",
    (date) => {
      expect(getRelativeTime(date, "en")).toBe("");
    },
  );
});

describe("getDurationTickStep", () => {
  it.each([
    [0, 1],
    [-1, 1],
    [Number.NaN, 1],
    [8, 1],
    [45, 10],
    [300, 60],
    [2000, 300],
    [36_000, 7200],
  ])("picks a %s second range step of %s seconds", (maxSeconds, expected) => {
    expect(getDurationTickStep(maxSeconds)).toBe(expected);
  });

  it("keeps the tick count within the requested budget", () => {
    expect(getDurationTickStep(2000) * 8).toBeGreaterThanOrEqual(2000);
  });

  it("falls back to an even split beyond the largest known step", () => {
    expect(getDurationTickStep(10_000_000)).toBe(1_250_000);
  });
});

describe("humanizeSeconds", () => {
  it.each([
    [3600, "1 hour"],
    [86_400, "1 day"],
    [3725.4, "1 hour, 2 minutes"],
    [0.083, "83 milliseconds"],
  ])("spells out %s seconds as %s in English", (seconds, expected) => {
    expect(humanizeSeconds(seconds, "en")).toBe(expected);
  });

  // The prose form is localized by the same CLDR data as the compact one.
  it.each([
    ["de", 3600, [["hour", 1]]],
    ["de", 86_400, [["day", 1]]],
    ["ru", 3600, [["hour", 1]]],
    [
      "fr",
      3725.4,
      [
        ["hour", 1],
        ["minute", 2],
      ],
    ],
  ] as Array<[string, number, Array<[Intl.NumberFormatOptions["unit"], number, number?]>]>)(
    "spells out %s duration of %s seconds",
    (locale, seconds, parts) => {
      expect(humanizeSeconds(seconds, locale)).toBe(expectDuration(locale, "long", parts));
    },
  );

  it("differs from the compact form and from English", () => {
    expect(humanizeSeconds(3600, "en")).not.toBe(renderDuration(3600, "en"));
    expect(humanizeSeconds(3600, "de")).not.toBe(humanizeSeconds(3600, "en"));
  });

  it.each([[null], [undefined], [Number.NaN], [Number.POSITIVE_INFINITY], [-5]])(
    "returns undefined without a usable interval (%s)",
    (seconds) => {
      expect(humanizeSeconds(seconds, "en")).toBeUndefined();
    },
  );
});
