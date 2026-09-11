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
import { describe, expect, it } from "vitest";

import type { CalendarDeadlineResponse, CalendarTimeRangeResponse } from "openapi/requests/types.gen";

import {
  buildDeadlineDateMap,
  calculateDataBounds,
  calculateRunCounts,
  createCalendarScale,
} from "./calendarUtils";
import type { RunCounts } from "./types";

const EMPTY_COLOR = { _dark: "gray.700", _light: "gray.100" };
const PLANNED_COLOR = { _dark: "stone.600", _light: "stone.500" };
const DEFAULT_TOTAL_COLOR = { _dark: "green.700", _light: "green.400" };
const DEFAULT_FAILED_COLOR = { _dark: "red.700", _light: "red.400" };
const DEFAULT_RUNNING_COLOR = { _dark: "cyan.700", _light: "cyan.400" };

const EMPTY_COUNTS: RunCounts = {
  failed: 0,
  planned: 0,
  queued: 0,
  running: 0,
  success: 0,
  total: 0,
};

const run = (
  state: CalendarTimeRangeResponse["state"],
  count: number,
  date = "2026-04-08T10:00:00Z",
): CalendarTimeRangeResponse => ({
  count,
  date,
  state,
});

describe("calculateRunCounts", () => {
  it("counts each calendar state and includes all states in total", () => {
    expect(
      calculateRunCounts([
        run("success", 2),
        run("failed", 1),
        run("running", 3),
        run("queued", 4),
        run("planned", 5),
      ]),
    ).toEqual({
      failed: 1,
      planned: 5,
      queued: 4,
      running: 3,
      success: 2,
      total: 15,
    });
  });
});

describe("calculateDataBounds", () => {
  it("uses total counts for total mode bounds", () => {
    expect(
      calculateDataBounds(
        [
          run("success", 2, "2026-04-08T10:00:00Z"),
          run("failed", 1, "2026-04-08T10:00:00Z"),
          run("running", 4, "2026-04-08T11:00:00Z"),
        ],
        { granularity: "hourly", timezone: "UTC", viewMode: "total" },
      ),
    ).toEqual({ maxCount: 4, minCount: 3 });
  });

  it("excludes queued runs from total mode bounds when actual runs are present", () => {
    expect(
      calculateDataBounds(
        [run("queued", 100, "2026-04-08T10:00:00Z"), run("success", 1, "2026-04-08T11:00:00Z")],
        { granularity: "hourly", timezone: "UTC", viewMode: "total" },
      ),
    ).toEqual({ maxCount: 1, minCount: 1 });
  });

  it("keeps queued-only total mode data from using an empty scale", () => {
    expect(
      calculateDataBounds([run("queued", 100)], {
        granularity: "hourly",
        timezone: "UTC",
        viewMode: "total",
      }),
    ).toEqual({
      maxCount: 100,
      minCount: 100,
    });
  });

  it("uses failed counts for failed mode bounds", () => {
    expect(
      calculateDataBounds(
        [
          run("success", 10, "2026-04-08T10:00:00Z"),
          run("failed", 2, "2026-04-08T10:00:00Z"),
          run("failed", 5, "2026-04-08T11:00:00Z"),
          run("queued", 20, "2026-04-08T11:00:00Z"),
        ],
        { granularity: "hourly", timezone: "UTC", viewMode: "failed" },
      ),
    ).toEqual({ maxCount: 5, minCount: 2 });
  });
});

describe("createCalendarScale", () => {
  it("returns the planned color for a planned-only cell", () => {
    const scale = createCalendarScale([run("planned", 1)], {
      granularity: "hourly",
      timezone: "UTC",
      viewMode: "total",
    });

    expect(scale.getColor({ ...EMPTY_COUNTS, planned: 1, total: 1 })).toEqual(PLANNED_COLOR);
  });

  it("returns the default total color for a success-only cell", () => {
    const scale = createCalendarScale([run("success", 1)], {
      granularity: "hourly",
      timezone: "UTC",
      viewMode: "total",
    });

    expect(scale.getColor({ ...EMPTY_COUNTS, success: 1, total: 1 })).toEqual(DEFAULT_TOTAL_COLOR);
  });

  it("returns the planned color for a queued-only cell in total mode", () => {
    const scale = createCalendarScale([run("queued", 1)], {
      granularity: "hourly",
      timezone: "UTC",
      viewMode: "total",
    });

    expect(scale.getColor({ ...EMPTY_COUNTS, queued: 1, total: 1 })).toEqual(PLANNED_COLOR);
  });

  it("returns a mixed color for planned and actual runs in total mode", () => {
    const scale = createCalendarScale([run("planned", 1), run("success", 1)], {
      granularity: "hourly",
      timezone: "UTC",
      viewMode: "total",
    });

    expect(scale.getColor({ ...EMPTY_COUNTS, planned: 1, success: 1, total: 2 })).toEqual({
      primary: DEFAULT_TOTAL_COLOR,
      secondary: PLANNED_COLOR,
    });
  });

  it("returns a mixed color for queued and actual runs in total mode", () => {
    const scale = createCalendarScale([run("queued", 1), run("success", 1)], {
      granularity: "hourly",
      timezone: "UTC",
      viewMode: "total",
    });

    expect(scale.getColor({ ...EMPTY_COUNTS, queued: 1, success: 1, total: 2 })).toEqual({
      primary: DEFAULT_TOTAL_COLOR,
      secondary: PLANNED_COLOR,
    });
  });

  it("returns the failed color for a failed-only cell in total mode", () => {
    const scale = createCalendarScale([run("failed", 1)], {
      granularity: "hourly",
      timezone: "UTC",
      viewMode: "total",
    });

    expect(scale.getColor({ ...EMPTY_COUNTS, failed: 1, total: 1 })).toEqual(DEFAULT_FAILED_COLOR);
  });

  it("returns a mixed red and green color for failed and success runs in total mode", () => {
    const scale = createCalendarScale([run("failed", 1), run("success", 1)], {
      granularity: "hourly",
      timezone: "UTC",
      viewMode: "total",
    });

    expect(scale.getColor({ ...EMPTY_COUNTS, failed: 1, success: 1, total: 2 })).toEqual({
      primary: DEFAULT_FAILED_COLOR,
      secondary: DEFAULT_TOTAL_COLOR,
    });
  });

  it("returns a mixed cyan and green color for running and success runs in total mode", () => {
    const scale = createCalendarScale([run("running", 1), run("success", 1)], {
      granularity: "hourly",
      timezone: "UTC",
      viewMode: "total",
    });

    expect(scale.getColor({ ...EMPTY_COUNTS, running: 1, success: 1, total: 2 })).toEqual({
      primary: DEFAULT_RUNNING_COLOR,
      secondary: DEFAULT_TOTAL_COLOR,
    });
  });

  it("returns a mixed cyan and red color for running and failed runs in total mode", () => {
    const scale = createCalendarScale([run("running", 1), run("failed", 1)], {
      granularity: "hourly",
      timezone: "UTC",
      viewMode: "total",
    });

    expect(scale.getColor({ ...EMPTY_COUNTS, failed: 1, running: 1, total: 2 })).toEqual({
      primary: DEFAULT_FAILED_COLOR,
      secondary: DEFAULT_RUNNING_COLOR,
    });
  });

  it("uses failed counts for failed mode", () => {
    const scale = createCalendarScale([run("success", 5), run("failed", 1)], {
      granularity: "hourly",
      timezone: "UTC",
      viewMode: "failed",
    });

    expect(scale.getColor({ ...EMPTY_COUNTS, success: 5, total: 5 })).toEqual(EMPTY_COLOR);
    expect(scale.getColor({ ...EMPTY_COUNTS, failed: 1, total: 1 })).toEqual(DEFAULT_FAILED_COLOR);
  });

  it("returns a mixed color for planned and failed runs in failed mode", () => {
    const scale = createCalendarScale([run("planned", 1), run("failed", 1)], {
      granularity: "hourly",
      timezone: "UTC",
      viewMode: "failed",
    });

    expect(scale.getColor({ ...EMPTY_COUNTS, failed: 1, planned: 1, total: 2 })).toEqual({
      primary: DEFAULT_FAILED_COLOR,
      secondary: PLANNED_COLOR,
    });
  });

  it("returns the planned color for a queued-only cell in failed mode", () => {
    const scale = createCalendarScale([run("queued", 1)], {
      granularity: "hourly",
      timezone: "UTC",
      viewMode: "failed",
    });

    expect(scale.getColor({ ...EMPTY_COUNTS, queued: 1, total: 1 })).toEqual(PLANNED_COLOR);
  });

  it("returns a mixed color for queued and failed runs in failed mode", () => {
    const scale = createCalendarScale([run("queued", 1), run("failed", 1)], {
      granularity: "hourly",
      timezone: "UTC",
      viewMode: "failed",
    });

    expect(scale.getColor({ ...EMPTY_COUNTS, failed: 1, queued: 1, total: 2 })).toEqual({
      primary: DEFAULT_FAILED_COLOR,
      secondary: PLANNED_COLOR,
    });
  });

  it("returns the correct gradient color when runs span across different dates", () => {
    const scale = createCalendarScale(
      [run("failed", 1, "2026-04-08T10:00:00Z"), run("failed", 5, "2026-04-09T10:00:00Z")],
      {
        granularity: "hourly",
        timezone: "UTC",
        viewMode: "total",
      },
    );

    const lowIntensityColor = { _dark: "red.900", _light: "red.200" };
    const highIntensityColor = { _dark: "red.300", _light: "red.800" };

    expect(scale.getColor({ ...EMPTY_COUNTS, failed: 1, total: 1 })).toEqual(lowIntensityColor);
    expect(scale.getColor({ ...EMPTY_COUNTS, failed: 5, total: 5 })).toEqual(highIntensityColor);
  });

  it("prioritizes failed over running over success when multiple actual states coexist with pending", () => {
    const scale = createCalendarScale([run("planned", 1), run("failed", 1), run("success", 1)], {
      granularity: "hourly",
      timezone: "UTC",
      viewMode: "total",
    });

    expect(scale.getColor({ ...EMPTY_COUNTS, failed: 1, planned: 1, success: 1, total: 3 })).toEqual({
      primary: DEFAULT_FAILED_COLOR,
      secondary: PLANNED_COLOR,
    });
  });

  it("returns an empty scale when no data is provided", () => {
    const scale = createCalendarScale([], {
      granularity: "hourly",
      timezone: "UTC",
      viewMode: "total",
    });

    expect(scale.type).toBe("empty");
    expect(scale.getColor(EMPTY_COUNTS)).toEqual(EMPTY_COLOR);
    expect(scale.legendItems).toEqual([{ color: EMPTY_COLOR, label: "0" }]);
  });

  it("prioritizes running and failed colors when failed, running, and success coexist without pending states", () => {
    const scale = createCalendarScale([run("failed", 1), run("running", 1), run("success", 1)], {
      granularity: "hourly",
      timezone: "UTC",
      viewMode: "total",
    });

    expect(scale.getColor({ ...EMPTY_COUNTS, failed: 1, running: 1, success: 1, total: 3 })).toEqual({
      primary: DEFAULT_FAILED_COLOR,
      secondary: DEFAULT_RUNNING_COLOR,
    });
  });

  it("returns the correct gradient color for failed mode when failed runs span across different dates", () => {
    const scale = createCalendarScale(
      [run("failed", 1, "2026-04-08T10:00:00Z"), run("failed", 10, "2026-04-09T10:00:00Z")],
      { granularity: "hourly", timezone: "UTC", viewMode: "failed" },
    );

    const lowIntensityFailedColor = { _dark: "red.900", _light: "red.200" };
    const highIntensityFailedColor = { _dark: "red.300", _light: "red.800" };

    expect(scale.getColor({ ...EMPTY_COUNTS, failed: 1, total: 1 })).toEqual(lowIntensityFailedColor);
    expect(scale.getColor({ ...EMPTY_COUNTS, failed: 10, total: 10 })).toEqual(highIntensityFailedColor);
  });
});

const buildDeadline = (date: string, missed: boolean, count = 1): CalendarDeadlineResponse => ({
  count,
  date,
  missed,
});

describe("buildDeadlineDateMap", () => {
  it("returns an empty map for empty input", () => {
    expect(buildDeadlineDateMap([], "UTC", "daily")).toEqual(new Map());
  });

  it("maps a missed deadline to the correct daily key", () => {
    const map = buildDeadlineDateMap([buildDeadline("2026-04-08T10:00:00Z", true, 2)], "UTC", "daily");

    expect(map.get("2026-04-08")).toEqual({ missed: 2, pending: 0 });
  });

  it("maps a pending deadline to the correct daily key", () => {
    const map = buildDeadlineDateMap([buildDeadline("2026-04-08T10:00:00Z", false, 3)], "UTC", "daily");

    expect(map.get("2026-04-08")).toEqual({ missed: 0, pending: 3 });
  });

  it("accumulates counts for multiple deadlines on the same day", () => {
    const map = buildDeadlineDateMap(
      [
        buildDeadline("2026-04-08T10:00:00Z", true, 1),
        buildDeadline("2026-04-08T14:00:00Z", false, 2),
        buildDeadline("2026-04-08T20:00:00Z", true, 3),
      ],
      "UTC",
      "daily",
    );

    expect(map.get("2026-04-08")).toEqual({ missed: 4, pending: 2 });
  });

  it("uses an hourly key for hourly granularity", () => {
    const map = buildDeadlineDateMap([buildDeadline("2026-04-08T10:30:00Z", true, 1)], "UTC", "hourly");

    expect(map.get("2026-04-08T10")).toEqual({ missed: 1, pending: 0 });
    expect(map.get("2026-04-08")).toBeUndefined();
  });

  it("respects timezone when grouping by day", () => {
    const map = buildDeadlineDateMap(
      [buildDeadline("2026-04-08T01:00:00Z", true, 1)],
      "America/New_York",
      "daily",
    );

    expect(map.get("2026-04-07")).toEqual({ missed: 1, pending: 0 });
    expect(map.get("2026-04-08")).toBeUndefined();
  });
});
