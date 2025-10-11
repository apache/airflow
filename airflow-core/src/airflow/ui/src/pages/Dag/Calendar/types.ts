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
import type { CalendarTimeRangeResponse } from "openapi/requests/types.gen";

export type DagRunState = "failed" | "planned" | "queued" | "running" | "success";

export type RunCounts = {
  failed: number;
  planned: number;
  queued: number;
  running: number;
  success: number;
  total: number;
};

export type CalendarCellData = {
  readonly counts: RunCounts;
  readonly date: string;
  readonly runs: Array<CalendarTimeRangeResponse>;
};

export type DayData = CalendarCellData;

export type HourData = {
  readonly hour: number;
} & CalendarCellData;

export type WeekData = Array<DayData>;

export type DailyCalendarData = Array<WeekData>;

export type HourlyCalendarData = {
  readonly days: Array<{
    readonly day: string;
    readonly hours: Array<HourData>;
  }>;
  readonly month: string;
};

export type CalendarGranularity = "daily" | "hourly";

export type CalendarColorMode = "failed" | "total";

export type LegendItem = {
  readonly color: string | { _dark: string; _light: string };
  readonly label: string;
};

export type CalendarScaleType = "empty" | "gradient" | "single_value";

export type CalendarScale = {
  readonly getColor: (counts: RunCounts) =>
    | string
    | { _dark: string; _light: string }
    | {
        actual: string | { _dark: string; _light: string };
        planned: string | { _dark: string; _light: string };
      };
  readonly legendItems: Array<LegendItem>;
  readonly type: CalendarScaleType;
};
