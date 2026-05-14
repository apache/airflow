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

import type { CalendarTimeRangeResponse } from "openapi/requests/types.gen";

import { calculateDataBounds, calculateRunCounts, createCalendarScale } from "./calendarUtils";
import type { RunCounts } from "./types";

const EMPTY_COLOR = { _dark: "gray.700", _light: "gray.100" };
const PLANNED_COLOR = { _dark: "stone.600", _light: "stone.500" };
const DEFAULT_TOTAL_COLOR = { _dark: "green.700", _light: "green.400" };
const DEFAULT_FAILED_COLOR = { _dark: "red.700", _light: "red.400" };

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
        "total",
        "hourly",
      ),
    ).toEqual({ maxCount: 4, minCount: 3 });
  });

  it("excludes queued runs from total mode bounds when actual runs are present", () => {
    expect(
      calculateDataBounds(
        [run("queued", 100, "2026-04-08T10:00:00Z"), run("success", 1, "2026-04-08T11:00:00Z")],
        "total",
        "hourly",
      ),
    ).toEqual({ maxCount: 1, minCount: 1 });
  });

  it("keeps queued-only total mode data from using an empty scale", () => {
    expect(calculateDataBounds([run("queued", 100)], "total", "hourly")).toEqual({
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
        "failed",
        "hourly",
      ),
    ).toEqual({ maxCount: 5, minCount: 2 });
  });
});

describe("createCalendarScale", () => {
  it("returns the planned color for a planned-only cell", () => {
    const scale = createCalendarScale([run("planned", 1)], "total", "hourly");

    expect(scale.getColor({ ...EMPTY_COUNTS, planned: 1, total: 1 })).toEqual(PLANNED_COLOR);
  });

  it("returns the default total color for a success-only cell", () => {
    const scale = createCalendarScale([run("success", 1)], "total", "hourly");

    expect(scale.getColor({ ...EMPTY_COUNTS, success: 1, total: 1 })).toEqual(DEFAULT_TOTAL_COLOR);
  });

  it("returns the planned color for a queued-only cell in total mode", () => {
    const scale = createCalendarScale([run("queued", 1)], "total", "hourly");

    expect(scale.getColor({ ...EMPTY_COUNTS, queued: 1, total: 1 })).toEqual(PLANNED_COLOR);
  });

  it("returns a mixed color for planned and actual runs in total mode", () => {
    const scale = createCalendarScale([run("planned", 1), run("success", 1)], "total", "hourly");

    expect(scale.getColor({ ...EMPTY_COUNTS, planned: 1, success: 1, total: 2 })).toEqual({
      actual: DEFAULT_TOTAL_COLOR,
      planned: PLANNED_COLOR,
    });
  });

  it("returns a mixed color for queued and actual runs in total mode", () => {
    const scale = createCalendarScale([run("queued", 1), run("success", 1)], "total", "hourly");

    expect(scale.getColor({ ...EMPTY_COUNTS, queued: 1, success: 1, total: 2 })).toEqual({
      actual: DEFAULT_TOTAL_COLOR,
      planned: PLANNED_COLOR,
    });
  });

  it("uses failed counts for failed mode", () => {
    const scale = createCalendarScale([run("success", 5), run("failed", 1)], "failed", "hourly");

    expect(scale.getColor({ ...EMPTY_COUNTS, success: 5, total: 5 })).toEqual(EMPTY_COLOR);
    expect(scale.getColor({ ...EMPTY_COUNTS, failed: 1, total: 1 })).toEqual(DEFAULT_FAILED_COLOR);
  });

  it("returns a mixed color for planned and failed runs in failed mode", () => {
    const scale = createCalendarScale([run("planned", 1), run("failed", 1)], "failed", "hourly");

    expect(scale.getColor({ ...EMPTY_COUNTS, failed: 1, planned: 1, total: 2 })).toEqual({
      actual: DEFAULT_FAILED_COLOR,
      planned: PLANNED_COLOR,
    });
  });

  it("returns the planned color for a queued-only cell in failed mode", () => {
    const scale = createCalendarScale([run("queued", 1)], "failed", "hourly");

    expect(scale.getColor({ ...EMPTY_COUNTS, queued: 1, total: 1 })).toEqual(PLANNED_COLOR);
  });

  it("returns a mixed color for queued and failed runs in failed mode", () => {
    const scale = createCalendarScale([run("queued", 1), run("failed", 1)], "failed", "hourly");

    expect(scale.getColor({ ...EMPTY_COUNTS, failed: 1, queued: 1, total: 2 })).toEqual({
      actual: DEFAULT_FAILED_COLOR,
      planned: PLANNED_COLOR,
    });
  });
});
