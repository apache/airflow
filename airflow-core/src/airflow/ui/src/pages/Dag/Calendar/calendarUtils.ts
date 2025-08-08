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

export const getCalendarCellColor = (runs: Array<CalendarTimeRangeResponse>): string => {
  if (runs.length === 0) {
    return CALENDAR_STATE_COLORS.empty;
  }

  const counts = calculateRunCounts(runs);

  if (counts.total === 0) {
    return CALENDAR_STATE_COLORS.empty;
  }

  // Priority states - show if any exist
  if (counts.queued > 0) {
    return CALENDAR_STATE_COLORS.queued.pure;
  }
  if (counts.running > 0) {
    return CALENDAR_STATE_COLORS.running.pure;
  }
  if (counts.planned > 0) {
    return CALENDAR_STATE_COLORS.planned.pure;
  }

  // Calculate rates for success/failed spectrum
  const successRate = counts.success / counts.total;

  if (successRate === 1) {
    return CALENDAR_STATE_COLORS.success.pure;
  }
  if (successRate >= SUCCESS_RATE_THRESHOLDS.HIGH) {
    return CALENDAR_STATE_COLORS.success.high;
  }
  if (successRate >= SUCCESS_RATE_THRESHOLDS.MEDIUM) {
    return CALENDAR_STATE_COLORS.success.medium;
  }
  if (successRate >= SUCCESS_RATE_THRESHOLDS.MODERATE) {
    return CALENDAR_STATE_COLORS.mixed.moderate;
  }
  if (successRate >= SUCCESS_RATE_THRESHOLDS.POOR) {
    return CALENDAR_STATE_COLORS.mixed.poor;
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
  const weeks = [];
  const startOfYear = dayjs().year(selectedYear).startOf("year");
  const endOfYear = dayjs().year(selectedYear).endOf("year");

  let currentDate = startOfYear.startOf("week");
  const endDate = endOfYear.endOf("week");

  while (currentDate.isBefore(endDate) || currentDate.isSame(endDate, "day")) {
    const week = [];

    for (let dayIndex = 0; dayIndex < 7; dayIndex += 1) {
      const dateStr = currentDate.format("YYYY-MM-DD");
      const runs = data.filter((run) => run.date.startsWith(dateStr));
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
  const monthStart = dayjs().year(selectedYear).month(selectedMonth).startOf("month");
  const monthEnd = dayjs().year(selectedYear).month(selectedMonth).endOf("month");
  const monthData = [];

  let currentDate = monthStart;

  while (currentDate.isSameOrBefore(monthEnd, "day")) {
    const dayHours = [];

    for (let hour = 0; hour < 24; hour += 1) {
      const hourStr = currentDate.hour(hour).format("YYYY-MM-DDTHH:00:00");
      const runs = data.filter((run) => run.date.startsWith(hourStr));
      const counts = calculateRunCounts(runs);

      dayHours.push({ counts, date: hourStr, hour, runs });
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

  const parts = [];

  if (counts.success > 0) {
    parts.push(`${counts.success} success`);
  }
  if (counts.failed > 0) {
    parts.push(`${counts.failed} failed`);
  }
  if (counts.planned > 0) {
    parts.push(`${counts.planned} planned`);
  }
  if (counts.running > 0) {
    parts.push(`${counts.running} running`);
  }
  if (counts.queued > 0) {
    parts.push(`${counts.queued} queued`);
  }

  return `${date}: ${counts.total} runs (${parts.join(", ")})`;
};
