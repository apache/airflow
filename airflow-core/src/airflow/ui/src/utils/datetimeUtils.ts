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

dayjs.extend(dayjsDuration);
dayjs.extend(relativeTime);
dayjs.extend(tz);

export const DATE_FORMAT = "YYYY-MM-DD";
export const DEFAULT_DATETIME_FORMAT = `${DATE_FORMAT} HH:mm:ss`;
export const DEFAULT_DATETIME_FORMAT_WITH_TZ = `${DEFAULT_DATETIME_FORMAT} z`;

export const renderDuration = (
  durationSeconds: dayjsDuration.Duration | number | null | undefined,
  withMilliseconds: boolean = true,
): string | undefined => {
  if (durationSeconds === null || durationSeconds === undefined) {
    return undefined;
  }

  // Handle floating point milliseconds
  const duration = dayjs.isDuration(durationSeconds)
    ? dayjs.duration(Math.round(durationSeconds.asMilliseconds()))
    : dayjs.duration(Number(durationSeconds.toFixed(3)), "seconds");

  if (duration.asMilliseconds() < 1) {
    return undefined;
  }

  // If under 60 seconds, render milliseconds
  if (duration.asSeconds() < 60 && duration.milliseconds() > 0 && withMilliseconds) {
    return duration.format("HH:mm:ss.SSS");
  }

  // If under 1 day, render as HH:mm:ss otherwise include the number of days
  return duration.asSeconds() < 86_400 ? duration.format("HH:mm:ss") : duration.format("D[d]HH:mm:ss");
};

// Chart axes need whole units at a glance; HH:mm:ss forces the reader to decode
// every tick to work out the magnitude.
export const renderCompactDuration = (durationSeconds: number): string => {
  if (!Number.isFinite(durationSeconds) || durationSeconds <= 0) {
    return "0s";
  }

  if (durationSeconds < 1) {
    return `${Math.round(durationSeconds * 1000)}ms`;
  }

  const duration = dayjs.duration(Math.round(durationSeconds), "seconds");
  const days = Math.floor(duration.asDays());
  const hours = duration.hours();
  const minutes = duration.minutes();
  const seconds = duration.seconds();

  if (days > 0) {
    return hours > 0 ? `${days}d ${hours}h` : `${days}d`;
  }

  if (hours > 0) {
    return minutes > 0 ? `${hours}h ${minutes}m` : `${hours}h`;
  }

  if (minutes > 0) {
    return seconds > 0 ? `${minutes}m ${seconds}s` : `${minutes}m`;
  }

  return `${seconds}s`;
};

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

export const getDuration = (
  startDate?: string | null,
  endDate?: string | null,
  withMilliseconds: boolean = true,
) => {
  if (startDate === undefined || startDate === null) {
    return undefined;
  }

  const end = endDate ?? dayjs().toISOString();
  const milliseconds = dayjs.duration(dayjs(end).diff(startDate));

  return renderDuration(milliseconds, withMilliseconds);
};

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

export const getRelativeTime = (date: string | null | undefined): string => {
  if (date === null || date === "" || date === undefined) {
    return "";
  }

  return dayjs(date).fromNow();
};

export const getTimezoneOffsetString = (timezone: string): string => dayjs().tz(timezone).format("Z");

export const getTimezoneTooltipLabel = (timezone: string): string => {
  const now = dayjs().tz(timezone);

  return `${timezone} — ${now.format(DEFAULT_DATETIME_FORMAT_WITH_TZ)}`;
};
