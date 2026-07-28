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
import tz from "dayjs/plugin/timezone";
import utc from "dayjs/plugin/utc";

import type { CalendarDeadlineResponse, CalendarTimeRangeResponse } from "openapi/requests/types.gen";
import { DATE_FORMAT } from "src/utils/datetimeUtils";

import type {
  DeadlineCounts,
  RunCounts,
  DailyCalendarData,
  HourlyCalendarData,
  CalendarColorMode,
  CalendarGranularity,
  CalendarScale,
  LegendItem,
} from "./types";

dayjs.extend(isSameOrBefore);
dayjs.extend(utc);
dayjs.extend(tz);

const HOURLY_KEY_FORMAT = `${DATE_FORMAT}THH`;

// Calendar color constants
export const PLANNED_COLOR = { _dark: "stone.600", _light: "stone.500" };
const EMPTY_COLOR = { _dark: "gray.700", _light: "gray.100" };
const RUNNING_COLOR = { _dark: "cyan.700", _light: "cyan.400" };

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

const getActualRunCount = (counts: RunCounts, viewMode: CalendarColorMode) =>
  viewMode === "total" ? counts.total - counts.planned - counts.queued : counts.failed;

const getPendingRunCount = (counts: RunCounts) => counts.planned + counts.queued;

const createDailyDataMap = (data: Array<CalendarTimeRangeResponse>, timezone: string) => {
  const dailyDataMap = new Map<string, Array<CalendarTimeRangeResponse>>();

  data.forEach((run) => {
    const dateStr = dayjs(run.date).tz(timezone).format(DATE_FORMAT);
    const dailyRuns = dailyDataMap.get(dateStr);

    if (dailyRuns) {
      dailyRuns.push(run);
    } else {
      dailyDataMap.set(dateStr, [run]);
    }
  });

  return dailyDataMap;
};

const createHourlyDataMap = (data: Array<CalendarTimeRangeResponse>, timezone: string) => {
  const hourlyDataMap = new Map<string, Array<CalendarTimeRangeResponse>>();

  data.forEach((run) => {
    const hourStr = dayjs(run.date).tz(timezone).format(HOURLY_KEY_FORMAT);
    const hourlyRuns = hourlyDataMap.get(hourStr);

    if (hourlyRuns) {
      hourlyRuns.push(run);
    } else {
      hourlyDataMap.set(hourStr, [run]);
    }
  });

  return hourlyDataMap;
};

export const buildDeadlineDateMap = (
  deadlines: Array<CalendarDeadlineResponse>,
  timezone: string,
  granularity: CalendarGranularity,
): Map<string, DeadlineCounts> => {
  const map = new Map<string, DeadlineCounts>();

  deadlines.forEach((deadline) => {
    const key =
      granularity === "daily"
        ? dayjs(deadline.date).tz(timezone).format(DATE_FORMAT)
        : dayjs(deadline.date).tz(timezone).format(HOURLY_KEY_FORMAT);

    const existing = map.get(key) ?? { missed: 0, pending: 0 };

    if (deadline.missed) {
      map.set(key, { ...existing, missed: existing.missed + deadline.count });
    } else {
      map.set(key, { ...existing, pending: existing.pending + deadline.count });
    }
  });

  return map;
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

  return counts;
};

type DailyOptions = {
  deadlineMap?: Map<string, DeadlineCounts>;
  selectedYear: number;
  timezone: string;
};

export const generateDailyCalendarData = (
  data: Array<CalendarTimeRangeResponse>,
  options: DailyOptions,
): DailyCalendarData => {
  const { deadlineMap, selectedYear, timezone } = options;
  const dailyDataMap = createDailyDataMap(data, timezone);

  const weeks = [];
  const startOfYear = dayjs().tz(timezone).year(selectedYear).startOf("year");
  const endOfYear = dayjs().tz(timezone).year(selectedYear).endOf("year");

  let currentDate = startOfYear.startOf("week");
  const endDate = endOfYear.endOf("week");

  while (currentDate.isBefore(endDate) || currentDate.isSame(endDate, "day")) {
    const week = [];

    for (let dayIndex = 0; dayIndex < 7; dayIndex += 1) {
      const dateStr = currentDate.format("YYYY-MM-DD");
      const runs = dailyDataMap.get(dateStr) ?? [];
      const counts = calculateRunCounts(runs);
      const deadlineCounts = deadlineMap?.get(dateStr);

      week.push({ counts, date: dateStr, deadlineCounts, runs });
      currentDate = currentDate.add(1, "day");
    }
    weeks.push(week);
  }

  return weeks;
};

type HourlyOptions = {
  deadlineMap?: Map<string, DeadlineCounts>;
  selectedMonth: number;
  selectedYear: number;
  timezone: string;
};

export const generateHourlyCalendarData = (
  data: Array<CalendarTimeRangeResponse>,
  options: HourlyOptions,
): HourlyCalendarData => {
  const { deadlineMap, selectedMonth, selectedYear, timezone } = options;
  const hourlyDataMap = createHourlyDataMap(data, timezone);

  const monthStart = dayjs().tz(timezone).year(selectedYear).month(selectedMonth).startOf("month");
  const monthEnd = dayjs().tz(timezone).year(selectedYear).month(selectedMonth).endOf("month");
  const monthData = [];

  let currentDate = monthStart;

  while (currentDate.isSameOrBefore(monthEnd, "day")) {
    const dayHours = [];

    for (let hour = 0; hour < 24; hour += 1) {
      const hourStr = currentDate.hour(hour).format("YYYY-MM-DDTHH");
      const runs = hourlyDataMap.get(hourStr) ?? [];
      const counts = calculateRunCounts(runs);
      const deadlineCounts = deadlineMap?.get(hourStr);

      dayHours.push({ counts, date: `${hourStr}:00:00`, deadlineCounts, hour, runs });
    }
    monthData.push({ day: currentDate.format("YYYY-MM-DD"), hours: dayHours });
    currentDate = currentDate.add(1, "day");
  }

  return { days: monthData, month: monthStart.format("MMM YYYY") };
};

type BoundsOptions = {
  granularity: CalendarGranularity;
  timezone: string;
  viewMode: CalendarColorMode;
};

export const calculateDataBounds = (
  data: Array<CalendarTimeRangeResponse>,
  options: BoundsOptions,
): { maxCount: number; minCount: number } => {
  const { granularity, timezone, viewMode } = options;

  if (data.length === 0) {
    return { maxCount: 0, minCount: 0 };
  }

  const counts: Array<number> = [];
  const pendingCounts: Array<number> = [];
  const mapCreator = granularity === "daily" ? createDailyDataMap : createHourlyDataMap;
  const dataMap = mapCreator(data, timezone);

  dataMap.forEach((runs) => {
    const runCounts = calculateRunCounts(runs);
    const targetCount = getActualRunCount(runCounts, viewMode);

    if (targetCount > 0) {
      counts.push(targetCount);
    } else {
      const pendingCount = getPendingRunCount(runCounts);

      if (pendingCount > 0) {
        pendingCounts.push(pendingCount);
      }
    }
  });

  if (counts.length === 0) {
    if (pendingCounts.length > 0) {
      return {
        maxCount: Math.max(...pendingCounts),
        minCount: Math.min(...pendingCounts),
      };
    }

    return { maxCount: 0, minCount: 0 };
  }

  return {
    maxCount: Math.max(...counts),
    minCount: Math.min(...counts),
  };
};

type ScaleOptions = {
  granularity: CalendarGranularity;
  timezone: string;
  viewMode: CalendarColorMode;
};

type ColorValue = string | { _dark: string; _light: string };

type ResolveColorParams = {
  failedColor: ColorValue;
  failedCount: number;
  hasPending: boolean;
  runningCount: number;
  successColor: ColorValue;
  successCount: number;
};

const resolveCellColor = ({
  failedColor,
  failedCount,
  hasPending,
  runningCount,
  successColor,
  successCount,
}: ResolveColorParams): ColorValue | { primary: ColorValue; secondary: ColorValue } => {
  const hasActual = failedCount > 0 || runningCount > 0 || successCount > 0;

  if (hasPending && hasActual) {
    let primaryColor: ColorValue = EMPTY_COLOR;

    if (failedCount > 0) {
      primaryColor = failedColor;
    } else if (runningCount > 0) {
      primaryColor = RUNNING_COLOR;
    } else if (successCount > 0) {
      primaryColor = successColor;
    }

    return {
      primary: primaryColor,
      secondary: PLANNED_COLOR,
    };
  }

  if (hasPending && !hasActual) {
    return PLANNED_COLOR;
  }

  if (hasActual) {
    if (failedCount > 0 && runningCount > 0) {
      return { primary: failedColor, secondary: RUNNING_COLOR };
    }

    if (failedCount > 0 && successCount > 0) {
      return { primary: failedColor, secondary: successColor };
    }

    if (runningCount > 0 && successCount > 0) {
      return { primary: RUNNING_COLOR, secondary: successColor };
    }

    if (failedCount > 0) {
      return failedColor;
    }
    if (runningCount > 0) {
      return RUNNING_COLOR;
    }
    if (successCount > 0) {
      return successColor;
    }
  }

  return EMPTY_COLOR;
};

export const createCalendarScale = (
  data: Array<CalendarTimeRangeResponse>,
  options: ScaleOptions,
): CalendarScale => {
  const { granularity, timezone, viewMode } = options;
  const { maxCount, minCount } = calculateDataBounds(data, { granularity, timezone, viewMode });

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
        const failedCount = counts.failed;
        const runningCount = viewMode === "total" ? counts.running : 0;
        const successCount = viewMode === "total" ? counts.success : 0;

        const hasPending = getPendingRunCount(counts) > 0;

        const failedColor = FAILURE_COLOR_INTENSITIES[2] ?? EMPTY_COLOR;
        const successColor = TOTAL_COLOR_INTENSITIES[2] ?? EMPTY_COLOR;

        return resolveCellColor({
          failedColor,
          failedCount,
          hasPending,
          runningCount,
          successColor,
          successCount,
        });
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
        primary: string | { _dark: string; _light: string };
        secondary: string | { _dark: string; _light: string };
      } => {
    const failedCount = counts.failed;
    const runningCount = viewMode === "total" ? counts.running : 0;
    const successCount = viewMode === "total" ? counts.success : 0;

    const hasPending = getPendingRunCount(counts) > 0;

    const getIntensityColor = (count: number, scheme: Array<ColorValue>) => {
      if (count === 0) {
        return scheme[0] ?? EMPTY_COLOR;
      }
      for (let index = uniqueThresholds.length - 1; index >= 1; index -= 1) {
        const threshold = uniqueThresholds[index];

        if (threshold !== undefined && count >= threshold) {
          return scheme[Math.min(index, scheme.length - 1)] ?? EMPTY_COLOR;
        }
      }

      return scheme[1] ?? EMPTY_COLOR;
    };

    const failedColor =
      failedCount > 0 ? getIntensityColor(failedCount, FAILURE_COLOR_INTENSITIES) : EMPTY_COLOR;
    const successColor =
      successCount > 0 ? getIntensityColor(successCount, TOTAL_COLOR_INTENSITIES) : EMPTY_COLOR;

    return resolveCellColor({
      failedColor,
      failedCount,
      hasPending,
      runningCount,
      successColor,
      successCount,
    });
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
