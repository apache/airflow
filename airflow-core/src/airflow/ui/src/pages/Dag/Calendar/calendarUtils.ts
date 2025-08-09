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
import isSameOrBefore from "dayjs/plugin/isSameOrBefore";

import type { CalendarTimeRangeResponse } from "openapi/requests/types.gen";

import { CALENDAR_STATE_COLORS, SUCCESS_RATE_THRESHOLDS } from "./constants";
import type { RunCounts, DailyCalendarData, HourlyCalendarData, CalendarCellData } from "./types";

dayjs.extend(isSameOrBefore);

const createDailyDataMap = (data: Array<CalendarTimeRangeResponse>) => {
  const dailyDataMap = new Map<string, Array<CalendarTimeRangeResponse>>();

  data.forEach((run) => {
    const dateStr = run.date.slice(0, 10); // "YYYY-MM-DD"
    const dailyRuns = dailyDataMap.get(dateStr);

    if (dailyRuns) {
      dailyRuns.push(run);
    } else {
      dailyDataMap.set(dateStr, [run]);
    }
  });

  return dailyDataMap;
};

const createHourlyDataMap = (data: Array<CalendarTimeRangeResponse>) => {
  const hourlyDataMap = new Map<string, Array<CalendarTimeRangeResponse>>();

  data.forEach((run) => {
    const hourStr = run.date.slice(0, 13); // "YYYY-MM-DDTHH"
    const hourlyRuns = hourlyDataMap.get(hourStr);

    if (hourlyRuns) {
      hourlyRuns.push(run);
    } else {
      hourlyDataMap.set(hourStr, [run]);
    }
  });

  return hourlyDataMap;
};

export const calculateRunCounts = (runs: Array<CalendarTimeRangeResponse>): RunCounts => {
  const counts: { [K in keyof RunCounts]: number } = {
    failed: 0,
    planned: 0,
    queued: 0,
    running: 0,
    success: 0,
    total: 0,
  };

  runs.forEach((run) => {
    const { count, state } = run;

    if (state in counts) {
      counts[state] += count;
    }
    counts.total += count;
  });

  return counts as RunCounts;
};

const PRIORITY_STATE_RULES = [
  { color: CALENDAR_STATE_COLORS.queued.pure, condition: (counts: RunCounts) => counts.queued > 0 },
  { color: CALENDAR_STATE_COLORS.running.pure, condition: (counts: RunCounts) => counts.running > 0 },
  { color: CALENDAR_STATE_COLORS.planned.pure, condition: (counts: RunCounts) => counts.planned > 0 },
] as const;

const SUCCESS_RATE_RULES = [
  { color: CALENDAR_STATE_COLORS.success.pure, threshold: 1 },
  { color: CALENDAR_STATE_COLORS.success.high, threshold: SUCCESS_RATE_THRESHOLDS.HIGH },
  { color: CALENDAR_STATE_COLORS.success.medium, threshold: SUCCESS_RATE_THRESHOLDS.MEDIUM },
  { color: CALENDAR_STATE_COLORS.mixed.moderate, threshold: SUCCESS_RATE_THRESHOLDS.MODERATE },
  { color: CALENDAR_STATE_COLORS.mixed.poor, threshold: SUCCESS_RATE_THRESHOLDS.POOR },
] as const;

export const getCalendarCellColor = (runs: Array<CalendarTimeRangeResponse>): string => {
  if (runs.length === 0) {
    return CALENDAR_STATE_COLORS.empty;
  }

  const counts = calculateRunCounts(runs);

  if (counts.total === 0) {
    return CALENDAR_STATE_COLORS.empty;
  }

  const priorityRule = PRIORITY_STATE_RULES.find((rule) => rule.condition(counts));

  if (priorityRule) {
    return priorityRule.color;
  }

  const successRate = counts.success / counts.total;
  const successRule = SUCCESS_RATE_RULES.find((rule) => successRate >= rule.threshold);

  if (successRule) {
    return successRule.color;
  }

  if (counts.failed > 0) {
    return CALENDAR_STATE_COLORS.failed.pure;
  }

  return CALENDAR_STATE_COLORS.other;
};

export const generateDailyCalendarData = (
  data: Array<CalendarTimeRangeResponse>,
  selectedYear: number,
): DailyCalendarData => {
  const dailyDataMap = createDailyDataMap(data);

  const weeks = [];
  const startOfYear = dayjs().year(selectedYear).startOf("year");
  const endOfYear = dayjs().year(selectedYear).endOf("year");

  let currentDate = startOfYear.startOf("week");
  const endDate = endOfYear.endOf("week");

  while (currentDate.isBefore(endDate) || currentDate.isSame(endDate, "day")) {
    const week = [];

    for (let dayIndex = 0; dayIndex < 7; dayIndex += 1) {
      const dateStr = currentDate.format("YYYY-MM-DD");
      const runs = dailyDataMap.get(dateStr) ?? [];
      const counts = calculateRunCounts(runs);

      week.push({ counts, date: dateStr, runs });
      currentDate = currentDate.add(1, "day");
    }
    weeks.push(week);
  }

  return weeks;
};

export const generateHourlyCalendarData = (
  data: Array<CalendarTimeRangeResponse>,
  selectedYear: number,
  selectedMonth: number,
): HourlyCalendarData => {
  const hourlyDataMap = createHourlyDataMap(data);

  const monthStart = dayjs().year(selectedYear).month(selectedMonth).startOf("month");
  const monthEnd = dayjs().year(selectedYear).month(selectedMonth).endOf("month");
  const monthData = [];

  let currentDate = monthStart;

  while (currentDate.isSameOrBefore(monthEnd, "day")) {
    const dayHours = [];

    for (let hour = 0; hour < 24; hour += 1) {
      const hourStr = currentDate.hour(hour).format("YYYY-MM-DDTHH");
      const runs = hourlyDataMap.get(hourStr) ?? [];
      const counts = calculateRunCounts(runs);

      dayHours.push({ counts, date: `${hourStr}:00:00`, hour, runs });
    }
    monthData.push({ day: currentDate.format("YYYY-MM-DD"), hours: dayHours });
    currentDate = currentDate.add(1, "day");
  }

  return { days: monthData, month: monthStart.format("MMM YYYY") };
};

export const createTooltipContent = (cellData: CalendarCellData): string => {
  const { counts, date } = cellData;

  if (counts.total === 0) {
    return `${date}: No runs`;
  }

  const parts = Object.entries(counts)
    .filter(([key, value]) => key !== "total" && value > 0)
    .map(([state, count]) => `${count} ${state}`);

  return `${date}: ${counts.total} runs (${parts.join(", ")})`;
};
