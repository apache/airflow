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
import { describe, expect, it } from "vitest";

import type { LightGridTaskInstanceSummary } from "openapi/requests/types.gen";
import type { GridTask } from "src/layouts/Details/Grid/utils";

import {
  type GanttDataItem,
  buildGanttRowSegments,
  buildGanttTimeAxisTicks,
  buildMaxTryByTaskId,
  GANTT_TIME_AXIS_TICK_COUNT,
  gridSummariesToTaskIdMap,
  transformGanttData,
} from "./utils";

describe("buildGanttTimeAxisTicks", () => {
  it("returns evenly spaced elapsed labels with edge alignment", () => {
    const minMs = 0;
    const maxMs = 60_000;
    const ticks = buildGanttTimeAxisTicks(minMs, maxMs);

    expect(ticks).toHaveLength(GANTT_TIME_AXIS_TICK_COUNT);
    expect(ticks[0]?.leftPct).toBe(0);
    expect(ticks[0]?.label).toBe("00:00:00");
    expect(ticks[0]?.labelAlign).toBe("left");
    expect(ticks[GANTT_TIME_AXIS_TICK_COUNT - 1]?.leftPct).toBe(100);
    expect(ticks[GANTT_TIME_AXIS_TICK_COUNT - 1]?.labelAlign).toBe("right");
    expect(ticks[GANTT_TIME_AXIS_TICK_COUNT - 1]?.label).toBe("00:01:00");
    expect(ticks[1]?.labelAlign).toBe("center");
    expect(ticks.every((tick) => typeof tick.label === "string" && tick.label.length > 0)).toBe(true);
  });

  it("supports a single tick", () => {
    const ticks = buildGanttTimeAxisTicks(1000, 1000, 1);

    expect(ticks).toHaveLength(1);
    expect(ticks[0]?.leftPct).toBe(0);
    expect(ticks[0]?.labelAlign).toBe("left");
    expect(ticks[0]?.label).toBe("00:00:00");
  });
});

describe("gridSummariesToTaskIdMap", () => {
  it("indexes summaries by task_id", () => {
    const summaries = [
      { state: null, task_id: "a" } as LightGridTaskInstanceSummary,
      { state: null, task_id: "b" } as LightGridTaskInstanceSummary,
    ];
    const map = gridSummariesToTaskIdMap(summaries);

    expect(map.get("a")).toBe(summaries[0]);
    expect(map.get("b")).toBe(summaries[1]);
    expect(map.size).toBe(2);
  });
});

describe("buildMaxTryByTaskId", () => {
  it("returns the maximum try number for each task", () => {
    const items: Array<GanttDataItem> = [
      { taskId: "t1", tryNumber: 1, x: [0, 1], y: "t1" },
      { taskId: "t1", tryNumber: 3, x: [0, 1], y: "t1" },
      { taskId: "t1", tryNumber: 2, x: [0, 1], y: "t1" },
      { taskId: "t2", tryNumber: 1, x: [0, 1], y: "t2" },
    ];
    const map = buildMaxTryByTaskId(items);

    expect(map.get("t1")).toBe(3);
    expect(map.get("t2")).toBe(1);
  });

  it("defaults to 1 when tryNumber is undefined", () => {
    const items: Array<GanttDataItem> = [{ taskId: "t1", x: [0, 1], y: "t1" }];
    const map = buildMaxTryByTaskId(items);

    expect(map.get("t1")).toBe(1);
  });

  it("returns an empty map for empty input", () => {
    expect(buildMaxTryByTaskId([]).size).toBe(0);
  });
});

describe("buildGanttRowSegments", () => {
  it("groups items by task id in flat node order", () => {
    const flatNodes: Array<GridTask> = [
      { depth: 0, id: "t1", is_mapped: false, label: "a" } as GridTask,
      { depth: 0, id: "t2", is_mapped: false, label: "b" } as GridTask,
    ];
    const items: Array<GanttDataItem> = [
      { taskId: "t2", x: [1_577_836_800_000, 1_577_923_200_000], y: "b" },
      { taskId: "t1", x: [1_577_836_800_000, 1_577_923_200_000], y: "a" },
    ];

    const segments = buildGanttRowSegments(flatNodes, items);

    expect(segments).toHaveLength(2);
    expect(segments[0]?.map((segment) => segment.taskId)).toEqual(["t1"]);
    expect(segments[1]?.map((segment) => segment.taskId)).toEqual(["t2"]);
  });
});

describe("transformGanttData", () => {
  it("returns no segments when the try has no schedule, queue, or start time", () => {
    const result = transformGanttData({
      allTries: [
        {
          end_date: null,
          is_mapped: false,
          queued_dttm: null,
          scheduled_dttm: null,
          start_date: null,
          state: null,
          task_display_name: "task_1",
          task_id: "task_1",
          try_number: 1,
        },
      ],
      flatNodes: [{ depth: 0, id: "task_1", is_mapped: false, label: "task_1" }],
      gridSummaries: [],
    });

    expect(result).toHaveLength(0);
  });

  it("includes running tasks with valid start_date and uses current time as end", () => {
    const before = dayjs();
    const result = transformGanttData({
      allTries: [
        {
          end_date: null,
          is_mapped: false,
          queued_dttm: null,
          scheduled_dttm: null,
          start_date: "2024-03-14T10:00:00+00:00",
          state: "running",
          task_display_name: "task_1",
          task_id: "task_1",
          try_number: 1,
        },
      ],
      flatNodes: [{ depth: 0, id: "task_1", is_mapped: false, label: "task_1" }],
      gridSummaries: [],
    });

    expect(result).toHaveLength(1);
    expect(result[0]?.state).toBe("running");
    const endTime = result[0]?.x[1] ?? 0;

    expect(endTime).toBeGreaterThanOrEqual(before.valueOf());
  });

  it("skips groups with null min_start_date or max_end_date", () => {
    const result = transformGanttData({
      allTries: [],
      flatNodes: [{ depth: 0, id: "group_1", is_mapped: false, isGroup: true, label: "group_1" }],
      gridSummaries: [
        {
          child_states: null,
          max_end_date: null,
          min_start_date: null,
          state: null,
          task_display_name: "group_1",
          task_id: "group_1",
        },
      ],
    });

    expect(result).toHaveLength(0);
  });

  it("uses millisecond timestamps for segment bounds", () => {
    const result = transformGanttData({
      allTries: [
        {
          end_date: "2024-03-14T10:05:00+00:00",
          is_mapped: false,
          queued_dttm: null,
          scheduled_dttm: null,
          start_date: "2024-03-14T10:00:00+00:00",
          state: "success",
          task_display_name: "task_1",
          task_id: "task_1",
          try_number: 1,
        },
      ],
      flatNodes: [{ depth: 0, id: "task_1", is_mapped: false, label: "task_1" }],
      gridSummaries: [],
    });

    expect(result).toHaveLength(1);
    expect(Number.isFinite(result[0]?.x[0])).toBe(true);
    expect(Number.isFinite(result[0]?.x[1])).toBe(true);
    expect(result[0]?.x[1]).toBeGreaterThanOrEqual(result[0]?.x[0] ?? 0);
  });

  it("produces 3 segments when scheduled_dttm and queued_dttm are present", () => {
    const result = transformGanttData({
      allTries: [
        {
          end_date: "2024-03-14T10:05:00+00:00",
          is_mapped: false,
          queued_dttm: "2024-03-14T09:59:00+00:00",
          scheduled_dttm: "2024-03-14T09:58:00+00:00",
          start_date: "2024-03-14T10:00:00+00:00",
          state: "success",
          task_display_name: "task_1",
          task_id: "task_1",
          try_number: 1,
        },
      ],
      flatNodes: [{ depth: 0, id: "task_1", is_mapped: false, label: "task_1" }],
      gridSummaries: [],
    });

    expect(result).toHaveLength(3);
    expect(result[0]?.state).toBe("scheduled");
    expect(result[1]?.state).toBe("queued");
    expect(result[2]?.state).toBe("success");
  });

  it("produces 2 segments when only queued_dttm is present", () => {
    const result = transformGanttData({
      allTries: [
        {
          end_date: "2024-03-14T10:05:00+00:00",
          is_mapped: false,
          queued_dttm: "2024-03-14T09:59:00+00:00",
          scheduled_dttm: null,
          start_date: "2024-03-14T10:00:00+00:00",
          state: "success",
          task_display_name: "task_1",
          task_id: "task_1",
          try_number: 1,
        },
      ],
      flatNodes: [{ depth: 0, id: "task_1", is_mapped: false, label: "task_1" }],
      gridSummaries: [],
    });

    expect(result).toHaveLength(2);
    expect(result[0]?.state).toBe("queued");
    expect(result[1]?.state).toBe("success");
  });

  it("produces 1 segment when scheduled_dttm and queued_dttm are null", () => {
    const result = transformGanttData({
      allTries: [
        {
          end_date: "2024-03-14T10:05:00+00:00",
          is_mapped: false,
          queued_dttm: null,
          scheduled_dttm: null,
          start_date: "2024-03-14T10:00:00+00:00",
          state: "success",
          task_display_name: "task_1",
          task_id: "task_1",
          try_number: 1,
        },
      ],
      flatNodes: [{ depth: 0, id: "task_1", is_mapped: false, label: "task_1" }],
      gridSummaries: [],
    });

    expect(result).toHaveLength(1);
    expect(result[0]?.state).toBe("success");
  });

  it("sorts multiple tries by try_number", () => {
    const result = transformGanttData({
      allTries: [
        {
          end_date: "2024-03-14T10:05:00+00:00",
          is_mapped: false,
          queued_dttm: null,
          scheduled_dttm: null,
          start_date: "2024-03-14T10:00:00+00:00",
          state: "failed",
          task_display_name: "task_1",
          task_id: "task_1",
          try_number: 2,
        },
        {
          end_date: "2024-03-14T09:55:00+00:00",
          is_mapped: false,
          queued_dttm: null,
          scheduled_dttm: null,
          start_date: "2024-03-14T09:50:00+00:00",
          state: "failed",
          task_display_name: "task_1",
          task_id: "task_1",
          try_number: 1,
        },
      ],
      flatNodes: [{ depth: 0, id: "task_1", is_mapped: false, label: "task_1" }],
      gridSummaries: [],
    });

    expect(result).toHaveLength(2);
    expect(result[0]?.tryNumber).toBe(1);
    expect(result[1]?.tryNumber).toBe(2);
  });
});
