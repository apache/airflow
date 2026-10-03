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
import "dayjs/locale/ar";
import dayjsDuration from "dayjs/plugin/duration";
import relativeTime from "dayjs/plugin/relativeTime";
import { afterAll, beforeAll, describe, expect, it } from "vitest";

import { humanizeSeconds } from "src/utils/datetimeUtils";

import { applyArabicPluralForms } from "./dayjsArabicPlurals";

dayjs.extend(dayjsDuration);
dayjs.extend(relativeTime);

// TypeScript's ES2022 lib does not declare Intl.DurationFormat, and browser support
// is still too new to ship against. It is used here only as a test oracle, so it is
// declared locally rather than widening the project's `lib`, and the suite skips the
// oracle checks on runtimes that lack it.
type DurationFormatConstructor = new (
  locales: string,
  options: { style: "long" },
) => { format: (duration: Record<string, number>) => string };

const durationFormat = (Intl as unknown as { DurationFormat?: DurationFormatConstructor }).DurationFormat;

// Counts stay inside each unit's humanize threshold so `humanize()` reaches the key
// under test instead of rolling up to the next unit.
const UNIT_WINDOWS = [
  { build: (count: number) => dayjs.duration(count, "minutes"), intlUnit: "minutes", max: 44 },
  { build: (count: number) => dayjs.duration(count, "hours"), intlUnit: "hours", max: 21 },
  { build: (count: number) => dayjs.duration(count, "days"), intlUnit: "days", max: 25 },
  { build: (count: number) => dayjs.duration(count, "months"), intlUnit: "months", max: 10 },
];

// The dual is the only form whose case is spelled out rather than carried by an unwritten
// vowel, so it is also the only count where these forms and Intl's can legitimately differ.
const FIRST_CASE_INVARIANT_COUNT = 3;

beforeAll(() => {
  applyArabicPluralForms();
  dayjs.locale("ar");
});

afterAll(() => {
  dayjs.locale("en");
});

describe("Arabic plural forms", () => {
  // Intl cannot arbitrate the dual, so every unit's is pinned here: it is the form the
  // oblique decision changes, and the one a future edit could silently put back into the
  // nominative.
  it.each([
    { expected: "دقيقتين", unit: "minutes" },
    { expected: "ساعتين", unit: "hours" },
    { expected: "يومين", unit: "days" },
    { expected: "شهرين", unit: "months" },
    { expected: "عامين", unit: "years" },
  ] as const)("inflects two $unit as the oblique dual $expected", ({ expected, unit }) => {
    expect(dayjs.duration(2, unit).humanize()).toBe(expected);
  });

  it.each([
    { count: 3, expected: "3 ساعات" },
    { count: 11, expected: "11 ساعة" },
    { count: 21, expected: "21 ساعة" },
  ])("inflects $count hours as $expected", ({ count, expected }) => {
    expect(dayjs.duration(count, "hours").humanize()).toBe(expected);
  });

  // Masculine nouns take tanwīn in the 11-99 form, which the feminine units hide.
  it.each([
    { count: 3, expected: "3 أيام" },
    { count: 11, expected: "11 يومًا" },
  ])("inflects $count days as $expected", ({ count, expected }) => {
    expect(dayjs.duration(count, "days").humanize()).toBe(expected);
  });

  // dayjs's `ar` locale words a year as عام where CLDR uses سنة. That vocabulary is a
  // choice the locale already made and is left alone; only the inflection is fixed,
  // so years are pinned to an explicit table rather than compared against Intl.
  it.each([
    { count: 3, expected: "3 أعوام" },
    { count: 11, expected: "11 عامًا" },
    { count: 100, expected: "100 عام" },
  ])("inflects $count years as $expected", ({ count, expected }) => {
    expect(dayjs.duration(count, "years").humanize()).toBe(expected);
  });

  // The tooltip reaches these forms through humanizeSeconds, not through dayjs
  // directly, so the seam between the two is worth pinning.
  it.each([
    { expected: "ساعتين", seconds: 7200 },
    { expected: "عامين", seconds: 63_072_000 },
  ])("humanizes $seconds seconds as $expected", ({ expected, seconds }) => {
    expect(humanizeSeconds(seconds)).toBe(expected);
  });

  // The oblique dual is chosen for the governed position, and dayjs's own past/future
  // wrappers put every relative time in one, so the wrapped form is what to assert.
  it.each([
    { expected: "منذ ساعتين", render: () => dayjs().subtract(2, "hour").fromNow() },
    { expected: "بعد يومين", render: () => dayjs().add(2, "day").fromNow() },
  ])("renders a relative time as $expected", ({ expected, render }) => {
    expect(render()).toBe(expected);
  });

  // Intl reads the same CLDR data these templates were transcribed from, so it is the
  // authority on which form each count takes. It formats a standalone duration and so
  // yields the nominative dual, which the cases above deliberately override with the
  // oblique; the sweep therefore starts at the first count where the two agree.
  describe.skipIf(durationFormat === undefined)("matches Intl.DurationFormat", () => {
    it.each(UNIT_WINDOWS)("for every count of $intlUnit in range", ({ build, intlUnit, max }) => {
      const format = new (durationFormat as DurationFormatConstructor)("ar", { style: "long" });

      for (let count = FIRST_CASE_INVARIANT_COUNT; count <= max; count += 1) {
        expect(build(count).humanize()).toBe(format.format({ [intlUnit]: count }));
      }
    });
  });
});
