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
import relativeTime from "dayjs/plugin/relativeTime";
import tz from "dayjs/plugin/timezone";
import i18n from "i18next";

dayjs.extend(dayjsDuration);
dayjs.extend(relativeTime);
dayjs.extend(tz);

export const DATE_FORMAT = "YYYY-MM-DD";
export const DEFAULT_DATETIME_FORMAT = `${DATE_FORMAT} HH:mm:ss`;
export const DEFAULT_DATETIME_FORMAT_WITH_TZ = `${DEFAULT_DATETIME_FORMAT} z`;

const DEFAULT_LOCALE = "en";
const SECONDS_PER_MINUTE = 60;
const SECONDS_PER_HOUR = 3600;
const SECONDS_PER_DAY = 86_400;

type DurationUnit = "day" | "hour" | "millisecond" | "minute" | "second";

type DurationPart = { fractionDigits?: number; unit: DurationUnit; value: number };

/** `narrow` ("1h 2m") suits dense tables and charts; `long` ("1 hour, 2 minutes") suits prose. */
type DurationStyle = "long" | "narrow";

// Durations render in every table row and chart tick callback, and Intl formatters are costly to
// construct, so instances are reused. A stored language Intl rejects must not blank out every
// duration in the UI, hence the fallback instead of letting the RangeError escape.
const unitFormatters = new Map<string, Intl.NumberFormat>();

const getUnitFormatter = (locale: string, style: DurationStyle, part: DurationPart): Intl.NumberFormat => {
  const { fractionDigits = 0, unit } = part;
  const key = `${locale}|${unit}|${fractionDigits}|${style}`;
  const cached = unitFormatters.get(key);

  if (cached !== undefined) {
    return cached;
  }

  const options: Intl.NumberFormatOptions = {
    maximumFractionDigits: fractionDigits,
    style: "unit",
    unit,
    unitDisplay: style,
  };
  let formatter: Intl.NumberFormat;

  try {
    formatter = new Intl.NumberFormat(locale, options);
  } catch {
    formatter = new Intl.NumberFormat(DEFAULT_LOCALE, options);
  }

  unitFormatters.set(key, formatter);

  return formatter;
};

const listFormatters = new Map<string, Intl.ListFormat>();

const getListFormatter = (locale: string, style: DurationStyle): Intl.ListFormat => {
  const key = `${locale}|${style}`;
  const cached = listFormatters.get(key);

  if (cached !== undefined) {
    return cached;
  }

  const options: Intl.ListFormatOptions = { style, type: "unit" };
  let formatter: Intl.ListFormat;

  try {
    formatter = new Intl.ListFormat(locale, options);
  } catch {
    formatter = new Intl.ListFormat(DEFAULT_LOCALE, options);
  }

  listFormatters.set(key, formatter);

  return formatter;
};

// Unit names, decimal separators, plural forms and the joiner all come from CLDR, so "1h 2m" is
// "1 ч 2 мин" in ru. This reproduces Intl.DurationFormat's narrow style exactly (verified across
// every locale we ship) without requiring it: that API needs Node 23+, above this package's
// engines floor, and Node 23 was never an LTS line. Exact wording also varies by the runtime's ICU
// version, so nothing may depend on a specific CLDR string.
const formatParts = (parts: Array<DurationPart>, locale: string, style: DurationStyle): string => {
  const formatted = parts.map((part) => getUnitFormatter(locale, style, part).format(part.value));

  return formatted.length > 1 ? getListFormatter(locale, style).format(formatted) : (formatted[0] ?? "");
};

// Durations carry roughly three significant digits at every magnitude, so a 83ms task and a
// three-day backfill are both legible without decoding zero-padded clock groups. Rounding at a
// band's precision can spill into the next band (59.96s is a minute, not "60.0s"), hence the
// recursion on the promoted value. Callers needing the unrounded number should surface it separately.
const getDurationParts = (seconds: number): Array<DurationPart> => {
  if (seconds === 0) {
    return [{ unit: "second", value: 0 }];
  }

  if (seconds < 1) {
    const milliseconds = Math.round(seconds * 1000);

    return milliseconds < 1000 ? [{ unit: "millisecond", value: milliseconds }] : getDurationParts(1);
  }

  if (seconds < SECONDS_PER_MINUTE) {
    // Two decimals under 10s, one above, keeps three significant digits either way.
    const fractionDigits = seconds < 10 ? 2 : 1;
    const rounded = Number(seconds.toFixed(fractionDigits));

    return rounded < SECONDS_PER_MINUTE
      ? [{ fractionDigits, unit: "second", value: rounded }]
      : getDurationParts(SECONDS_PER_MINUTE);
  }

  if (seconds < SECONDS_PER_HOUR) {
    const minutes = Math.floor(seconds / SECONDS_PER_MINUTE);
    const remainingSeconds = Math.round(seconds - minutes * SECONDS_PER_MINUTE);

    if (remainingSeconds === SECONDS_PER_MINUTE) {
      return getDurationParts((minutes + 1) * SECONDS_PER_MINUTE);
    }

    return remainingSeconds > 0
      ? [
          { unit: "minute", value: minutes },
          { unit: "second", value: remainingSeconds },
        ]
      : [{ unit: "minute", value: minutes }];
  }

  if (seconds < SECONDS_PER_DAY) {
    const hours = Math.floor(seconds / SECONDS_PER_HOUR);
    const remainingMinutes = Math.round((seconds - hours * SECONDS_PER_HOUR) / SECONDS_PER_MINUTE);

    if (remainingMinutes === SECONDS_PER_MINUTE) {
      return getDurationParts((hours + 1) * SECONDS_PER_HOUR);
    }

    return remainingMinutes > 0
      ? [
          { unit: "hour", value: hours },
          { unit: "minute", value: remainingMinutes },
        ]
      : [{ unit: "hour", value: hours }];
  }

  const days = Math.floor(seconds / SECONDS_PER_DAY);
  const remainingHours = Math.round((seconds - days * SECONDS_PER_DAY) / SECONDS_PER_HOUR);

  if (remainingHours === 24) {
    return getDurationParts((days + 1) * SECONDS_PER_DAY);
  }

  return remainingHours > 0
    ? [
        { unit: "day", value: days },
        { unit: "hour", value: remainingHours },
      ]
    : [{ unit: "day", value: days }];
};

const formatDuration = (
  duration: dayjsDuration.Duration | number | null | undefined,
  locale: string,
  style: DurationStyle,
): string | undefined => {
  if (duration === null || duration === undefined) {
    return undefined;
  }

  const seconds = dayjs.isDuration(duration) ? duration.asSeconds() : Number(duration);

  if (!Number.isFinite(seconds) || seconds < 0) {
    return undefined;
  }

  // Below a millisecond the digits are timestamp resolution and clock skew rather than signal. "<"
  // is mathematical notation, not prose, so CLDR has no pattern for it and none is needed.
  if (seconds > 0 && seconds < 0.001) {
    return `<${formatParts([{ unit: "millisecond", value: 1 }], locale, style)}`;
  }

  return formatParts(getDurationParts(seconds), locale, style);
};

/**
 * Formats a duration for display, localized to the active UI language.
 *
 * `locale` defaults to the current i18next language and exists so tests and callers outside React
 * can pin it; pass it rather than reaching for the raw seconds.
 */
export const renderDuration = (
  duration: dayjsDuration.Duration | number | null | undefined,
  locale: string = i18n.language || DEFAULT_LOCALE,
): string | undefined => formatDuration(duration, locale, "narrow");

/** Spelled-out duration for prose, where "1 hour" reads better than the table form "1h". */
export const humanizeSeconds = (
  seconds: number | null | undefined,
  locale: string = i18n.language || DEFAULT_LOCALE,
): string | undefined => formatDuration(seconds, locale, "long");

// Chart.js picks decimal steps, which on a time axis reads as 26m 40s / 33m 20s.
// Snapping to units people actually count in keeps the ticks legible.
const DURATION_TICK_STEPS_SECONDS = [
  1, 2, 5, 10, 15, 30, 60, 120, 300, 600, 900, 1800, 3600, 7200, 10_800, 21_600, 43_200, 86_400, 172_800,
  604_800,
];

export const getDurationTickStep = (maxSeconds: number, maxTicks = 8): number => {
  if (!Number.isFinite(maxSeconds) || maxSeconds <= 0) {
    return 1;
  }

  return (
    DURATION_TICK_STEPS_SECONDS.find((candidate) => maxSeconds / candidate <= maxTicks) ??
    Math.ceil(maxSeconds / maxTicks)
  );
};

/** Elapsed seconds between two timestamps, counting an absent `endDate` as still running. */
export const getElapsedSeconds = (startDate?: string | null, endDate?: string | null): number | undefined => {
  if (startDate === undefined || startDate === null) {
    return undefined;
  }

  const start = dayjs(startDate);
  const end = endDate === undefined || endDate === null ? dayjs() : dayjs(endDate);

  return start.isValid() && end.isValid() ? dayjs.duration(end.diff(start)).asSeconds() : undefined;
};

export const getDuration = (startDate?: string | null, endDate?: string | null, locale?: string) =>
  renderDuration(getElapsedSeconds(startDate, endDate), locale);

export const formatDate = (
  date: number | string | null | undefined,
  timezone: string,
  format: string = DEFAULT_DATETIME_FORMAT,
) => {
  if (date === null || date === undefined || !dayjs(date).isValid()) {
    return dayjs().tz(timezone).format(format);
  }

  return dayjs(date).tz(timezone).format(format);
};

// Ordered largest first so the first unit the difference reaches wins: "45 minutes ago" rather than
// "2700 seconds ago". Months and years use the mean Gregorian lengths CLDR assumes for relative
// phrasing. Anything under a minute falls through to seconds.
const RELATIVE_TIME_UNITS: Array<{ seconds: number; unit: Intl.RelativeTimeFormatUnit }> = [
  { seconds: 31_557_600, unit: "year" },
  { seconds: 2_629_800, unit: "month" },
  { seconds: SECONDS_PER_DAY * 7, unit: "week" },
  { seconds: SECONDS_PER_DAY, unit: "day" },
  { seconds: SECONDS_PER_HOUR, unit: "hour" },
  { seconds: SECONDS_PER_MINUTE, unit: "minute" },
  { seconds: 1, unit: "second" },
];

const RELATIVE_TIME_FALLBACK_UNIT = { seconds: 1, unit: "second" } as const;

const relativeTimeFormatters = new Map<string, Intl.RelativeTimeFormat>();

const getRelativeTimeFormatter = (locale: string): Intl.RelativeTimeFormat => {
  const cached = relativeTimeFormatters.get(locale);

  if (cached !== undefined) {
    return cached;
  }

  const options: Intl.RelativeTimeFormatOptions = { numeric: "auto" };
  let formatter: Intl.RelativeTimeFormat;

  try {
    formatter = new Intl.RelativeTimeFormat(locale, options);
  } catch {
    formatter = new Intl.RelativeTimeFormat(DEFAULT_LOCALE, options);
  }

  relativeTimeFormatters.set(locale, formatter);

  return formatter;
};

export const getRelativeTime = (
  date: string | null | undefined,
  locale: string = i18n.language || DEFAULT_LOCALE,
): string => {
  if (date === null || date === "" || date === undefined || !dayjs(date).isValid()) {
    return "";
  }

  const elapsed = dayjs(date).diff(dayjs(), "second", true);
  const magnitude = Math.abs(elapsed);
  const { seconds, unit } =
    RELATIVE_TIME_UNITS.find((candidate) => magnitude >= candidate.seconds) ?? RELATIVE_TIME_FALLBACK_UNIT;

  return getRelativeTimeFormatter(locale).format(Math.round(elapsed / seconds), unit);
};

export const getTimezoneOffsetString = (timezone: string): string => dayjs().tz(timezone).format("Z");

export const getTimezoneTooltipLabel = (timezone: string): string => {
  const now = dayjs().tz(timezone);

  return `${timezone} — ${now.format(DEFAULT_DATETIME_FORMAT_WITH_TZ)}`;
};
