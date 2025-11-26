/* eslint-disable max-lines */

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
  CalendarColorMode,
  CalendarGranularity,
  CalendarScale,
  LegendItem,
} from "./types";

dayjs.extend(isSameOrBefore);

// Calendar color constants
export const PLANNED_COLOR = { _dark: "stone.600", _light: "stone.500" };
const EMPTY_COLOR = { _dark: "gray.700", _light: "gray.100" };

const TOTAL_COLOR_INTENSITIES = [
  EMPTY_COLOR, // 0
  { _dark: "green.900", _light: "green.200" },
  { _dark: "green.700", _light: "green.400" },
  { _dark: "green.500", _light: "green.600" },
  { _dark: "green.300", _light: "green.800" },
];

const FAILURE_COLOR_INTENSITIES = [
  EMPTY_COLOR, // 0
  { _dark: "red.900", _light: "red.200" },
  { _dark: "red.700", _light: "red.400" },
  { _dark: "red.500", _light: "red.600" },
  { _dark: "red.300", _light: "red.800" },
];

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

export const calculateDataBounds = (
  data: Array<CalendarTimeRangeResponse>,
  viewMode: CalendarColorMode,
  granularity: CalendarGranularity,
): { maxCount: number; minCount: number } => {
  if (data.length === 0) {
    return { maxCount: 0, minCount: 0 };
  }

  const counts: Array<number> = [];
  const mapCreator = granularity === "daily" ? createDailyDataMap : createHourlyDataMap;
  const dataMap = mapCreator(data);

  dataMap.forEach((runs) => {
    const runCounts = calculateRunCounts(runs);
    const targetCount = viewMode === "total" ? runCounts.total : runCounts.failed;

    if (targetCount > 0) {
      counts.push(targetCount);
    }
  });

  if (counts.length === 0) {
    return { maxCount: 0, minCount: 0 };
  }

  return {
    maxCount: Math.max(...counts),
    minCount: Math.min(...counts),
  };
};

export const createCalendarScale = (
  data: Array<CalendarTimeRangeResponse>,
  viewMode: CalendarColorMode,
  granularity: CalendarGranularity,
): CalendarScale => {
  const { maxCount, minCount } = calculateDataBounds(data, viewMode, granularity);

  // Handle empty data case
  if (maxCount === 0) {
    return {
      getColor: () => EMPTY_COLOR,
      legendItems: [{ color: EMPTY_COLOR, label: "0" }],
      type: "empty",
    };
  }

  // Handle single value case
  if (minCount === maxCount) {
    const singleColor =
      (viewMode === "total" ? TOTAL_COLOR_INTENSITIES[2] : FAILURE_COLOR_INTENSITIES[2]) ?? EMPTY_COLOR;

    return {
      getColor: (counts: RunCounts) => {
        const actualCount = viewMode === "total" ? counts.total - counts.planned : counts.failed;
        const hasPlanned = counts.planned > 0;
        const hasActual = actualCount > 0;

        if (hasPlanned && hasActual) {
          return {
            actual: singleColor,
            planned: PLANNED_COLOR,
          };
        }

        if (hasPlanned && !hasActual) {
          return PLANNED_COLOR;
        }

        return actualCount === 0 ? EMPTY_COLOR : singleColor;
      },
      legendItems: [
        { color: EMPTY_COLOR, label: "0" },
        { color: singleColor, label: maxCount.toString() },
      ],
      type: "single_value",
    };
  }

  // Handle gradient case - create dynamic thresholds
  const range = maxCount - minCount;
  const colorScheme = viewMode === "total" ? TOTAL_COLOR_INTENSITIES : FAILURE_COLOR_INTENSITIES;

  const thresholds = [
    0,
    Math.max(1, Math.ceil(minCount + range * 0.25)),
    Math.max(2, Math.ceil(minCount + range * 0.5)),
    Math.max(3, Math.ceil(minCount + range * 0.75)),
    maxCount,
  ];

  const uniqueThresholds = [...new Set(thresholds)].sort((first, second) => first - second);

  const getColor = (
    counts: RunCounts,
  ):
    | string
    | { _dark: string; _light: string }
    | {
        actual: string | { _dark: string; _light: string };
        planned: string | { _dark: string; _light: string };
      } => {
    const actualCount = viewMode === "total" ? counts.total - counts.planned : counts.failed;
    const hasPlanned = counts.planned > 0;
    const hasActual = actualCount > 0;

    if (hasPlanned && hasActual) {
      let actualColor = colorScheme[0] ?? EMPTY_COLOR;

      for (let index = uniqueThresholds.length - 1; index >= 1; index -= 1) {
        const threshold = uniqueThresholds[index];

        if (threshold !== undefined && actualCount >= threshold) {
          actualColor = colorScheme[Math.min(index, colorScheme.length - 1)] ?? EMPTY_COLOR;
          break;
        }
      }

      if (actualCount > 0 && actualColor === colorScheme[0]) {
        actualColor = colorScheme[1] ?? EMPTY_COLOR;
      }

      return {
        actual: actualColor,
        planned: PLANNED_COLOR,
      };
    }

    if (hasPlanned && !hasActual) {
      return PLANNED_COLOR;
    }

    const targetCount = actualCount;

    if (targetCount === 0) {
      return colorScheme[0] ?? EMPTY_COLOR;
    }

    for (let index = uniqueThresholds.length - 1; index >= 1; index -= 1) {
      const threshold = uniqueThresholds[index];

      if (threshold !== undefined && targetCount >= threshold) {
        return colorScheme[Math.min(index, colorScheme.length - 1)] ?? EMPTY_COLOR;
      }
    }

    return colorScheme[1] ?? EMPTY_COLOR;
  };

  const legendItems: Array<LegendItem> = [];

  uniqueThresholds.forEach((threshold, index, thresholdsArray) => {
    const nextThreshold = thresholdsArray[index + 1];

    let label: string;

    if (index === 0) {
      label = "0";
    } else if (index === thresholdsArray.length - 1) {
      label = `${threshold}+`;
    } else if (nextThreshold !== undefined && threshold + 1 === nextThreshold) {
      label = threshold.toString();
    } else if (nextThreshold === undefined) {
      label = `${threshold}+`;
    } else {
      label = `${threshold}-${nextThreshold - 1}`;
    }

    const color = colorScheme[Math.min(index, colorScheme.length - 1)] ?? EMPTY_COLOR;

    legendItems.push({
      color,
      label,
    });
  });

  return {
    getColor,
    legendItems,
    type: "gradient",
  };
};
