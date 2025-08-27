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

import type {
  RunCounts,
  DailyCalendarData,
  HourlyCalendarData,
  CalendarCellData,
  CalendarColorMode,
} from "./types";

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

const TOTAL_COLOR_INTENSITIES = [
  { _dark: "gray.700", _light: "gray.100" }, // 0 runs
  { _dark: "green.300", _light: "green.200" }, // 1-5 runs
  { _dark: "green.500", _light: "green.400" }, // 6-15 runs
  { _dark: "green.700", _light: "green.600" }, // 16-25 runs
  { _dark: "green.900", _light: "green.800" }, // 26+ runs
] as const;

const FAILURE_COLOR_INTENSITIES = [
  { _dark: "gray.700", _light: "gray.100" }, // 0 failures
  { _dark: "red.300", _light: "red.200" }, // 1-2 failures
  { _dark: "red.500", _light: "red.400" }, // 3-5 failures
  { _dark: "red.700", _light: "red.600" }, // 6-10 failures
  { _dark: "red.900", _light: "red.800" }, // 11+ failures
] as const;

const PLANNED_COLOR = { _dark: "scheduled.600", _light: "scheduled.200" };

const getIntensityLevel = (count: number, mode: CalendarColorMode): number => {
  if (count === 0) {
    return 0;
  }

  if (mode === "total") {
    if (count <= 5) {
      return 1;
    }
    if (count <= 15) {
      return 2;
    }
    if (count <= 25) {
      return 3;
    }

    return 4;
  } else {
    // failed runs mode
    if (count <= 2) {
      return 1;
    }
    if (count <= 5) {
      return 2;
    }
    if (count <= 10) {
      return 3;
    }

    return 4;
  }
};

export const getCalendarCellColor = (
  runs: Array<CalendarTimeRangeResponse>,
  colorMode: CalendarColorMode = "total",
): string | { _dark: string; _light: string } => {
  if (runs.length === 0) {
    return { _dark: "gray.700", _light: "gray.100" };
  }

  const counts = calculateRunCounts(runs);

  if (counts.planned > 0) {
    return PLANNED_COLOR;
  }

  const targetCount = colorMode === "total" ? counts.total : counts.failed;
  const intensityLevel = getIntensityLevel(targetCount, colorMode);
  const colorScheme = colorMode === "total" ? TOTAL_COLOR_INTENSITIES : FAILURE_COLOR_INTENSITIES;

  return colorScheme[intensityLevel] ?? { _dark: "gray.700", _light: "gray.100" };
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
