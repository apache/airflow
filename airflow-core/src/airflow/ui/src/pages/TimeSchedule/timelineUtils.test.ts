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

import { buildDayRowLayouts, getTimelineBarMinimumWidth, getVisualDurationWidth } from "./timelineUtils";
import type { TimelineRow } from "./types";

const row: TimelineRow = {
  dag_display_name: "example_dag",
  dag_id: "example_dag",
  is_time_scheduled: true,
  items: [
    {
      dag_display_name: "example_dag",
      dag_id: "example_dag",
      dag_run_id: "run-1",
      duration_ms: 60_000,
      end_date: "2024-01-01T00:01:00Z",
      is_placeholder: false,
      is_planned: false,
      is_time_scheduled: true,
      run_count: 1,
      start_date: "2024-01-01T00:00:00Z",
      state: "success",
    },
    {
      dag_display_name: "example_dag",
      dag_id: "example_dag",
      dag_run_id: "run-2",
      duration_ms: 60_000,
      end_date: "2024-01-01T00:02:00Z",
      is_placeholder: false,
      is_planned: false,
      is_time_scheduled: true,
      run_count: 1,
      start_date: "2024-01-01T00:01:00Z",
      state: "success",
    },
  ],
};

describe("Time Schedule timeline layout", () => {
  it("keeps consecutive minute-long bars in the same lane when their time ranges do not overlap", () => {
    const [layout] = buildDayRowLayouts({
      rows: [row],
      selectedTimezone: "UTC",
      timelineWidth: 86_400,
    });

    expect(layout?.items.map(({ lane }) => lane)).toEqual([0, 0]);
  });

  it.each([60_000, 30 * 60_000, 60 * 60_000])("uses time-proportional width for %s ms", (durationMs) => {
    expect(getVisualDurationWidth(durationMs, "en")).toBe(
      `max(${getTimelineBarMinimumWidth(durationMs, "en")}px, ${(durationMs / 86_400_000) * 100}%)`,
    );
  });

  it.each([
    [50_000, 48],
    [6 * 60_000 + 10_000, 38],
    [50 * 60_000 + 10_000, 48],
    [3_600_000 + 2 * 60_000 + 10_000, 68],
  ])("reserves only the compact label and icon width for %s ms", (durationMs, width) => {
    expect(getTimelineBarMinimumWidth(durationMs, "en")).toBe(width);
  });

  it("uses the label minimum width for lane placement", () => {
    const [layout] = buildDayRowLayouts({
      locale: "en",
      rows: [row],
      selectedTimezone: "UTC",
      timelineWidth: 1440,
    });

    expect(layout?.items.map(({ lane }) => lane)).toEqual([0, 1]);
  });

  it("separates overlapping planned hour-long runs by their full duration", () => {
    const [layout] = buildDayRowLayouts({
      rows: [
        { ...row, items: row.items.map((item) => ({ ...item, duration_ms: 3_600_000, is_planned: true })) },
      ],
      selectedTimezone: "UTC",
      timelineWidth: 1440,
    });

    expect(layout?.items.map(({ lane }) => lane)).toEqual([0, 1]);
  });
});
