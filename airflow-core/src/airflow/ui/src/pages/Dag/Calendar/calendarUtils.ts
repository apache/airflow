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
  { color: "queued.600", condition: (counts: RunCounts) => counts.queued > 0 },
  { color: "blue.400", condition: (counts: RunCounts) => counts.running > 0 },
  {
    color: { _dark: "scheduled.600", _light: "scheduled.200" },
    condition: (counts: RunCounts) => counts.planned > 0,
  },
] as const;

const SUCCESS_RATE_RULES = [
  { color: "success.600", threshold: 1 },
  { color: "success.500", threshold: 0.8 },
  { color: "success.400", threshold: 0.6 },
  { color: "up_for_retry.500", threshold: 0.4 },
  { color: "upstream_failed.500", threshold: 0.2 },
] as const;

export const getCalendarCellColor = (
  runs: Array<CalendarTimeRangeResponse>,
): string | { _dark: string; _light: string } => {
  if (runs.length === 0) {
    return "bg.muted";
  }

  const counts = calculateRunCounts(runs);

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
    return "failed.600";
  }

  return "gray.400";
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
