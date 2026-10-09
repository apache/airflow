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
import { describe, expect, it, vi } from "vitest";

import type { LightGridTaskInstanceSummary } from "openapi/requests/types.gen";

import type { GridTask } from "src/layouts/Details/Grid/utils";

import {
  type GanttDataItem,
  buildGanttRowSegments,
  buildGanttTimeAxisTicks,
  buildMaxTryByKey,
  GANTT_TIME_AXIS_TICK_COUNT,
  gridSummariesToTaskIdMap,
  getGanttSegmentTo,
  transformGanttData,
} from "./utils";

describe("buildGanttTimeAxisTicks", () => {
  it("returns evenly spaced elapsed labels with edge alignment", () => {
    const minMs = 0;
    const maxMs = 60_000;
    const ticks = buildGanttTimeAxisTicks(minMs, maxMs);

    expect(ticks).toHaveLength(GANTT_TIME_AXIS_TICK_COUNT);
    expect(ticks[0]?.leftPct).toBe(0);
    expect(ticks[0]?.label).toBe("0s");
    expect(ticks[0]?.labelAlign).toBe("left");
    expect(ticks[GANTT_TIME_AXIS_TICK_COUNT - 1]?.leftPct).toBe(100);
    expect(ticks[GANTT_TIME_AXIS_TICK_COUNT - 1]?.labelAlign).toBe("right");
    expect(ticks[GANTT_TIME_AXIS_TICK_COUNT - 1]?.label).toBe("1m");
    expect(ticks[1]?.labelAlign).toBe("center");
    expect(ticks.every((tick) => typeof tick.label === "string" && tick.label.length > 0)).toBe(true);
  });

  it("localizes tick labels with the given locale", () => {
    const ticks = buildGanttTimeAxisTicks(0, 60_000, { locale: "de", tickCount: 2 });
    const inEnglish = buildGanttTimeAxisTicks(0, 60_000, { locale: "en", tickCount: 2 });

    expect(ticks.at(-1)?.label).toBe(
      new Intl.NumberFormat("de", { style: "unit", unit: "minute", unitDisplay: "narrow" }).format(1),
    );
    expect(ticks.at(-1)?.label).not.toBe(inEnglish.at(-1)?.label);
  });

  it("snaps ticks to counted units rather than dividing the raw span", () => {
    // A 7s span divided evenly used to read 0s | 1.17s | 2.33s | 3.5s ...
    const labels = buildGanttTimeAxisTicks(0, 7000, { locale: "en", tickCount: 8 }).map((tick) => tick.label);

    expect(labels).toStrictEqual(["0s", "1s", "2s", "3s", "4s", "5s", "6s", "7s"]);
  });

  it("supports a single tick", () => {
    const ticks = buildGanttTimeAxisTicks(1000, 1000, { tickCount: 1 });

    expect(ticks).toHaveLength(1);
    expect(ticks[0]?.leftPct).toBe(0);
    expect(ticks[0]?.labelAlign).toBe("left");
    expect(ticks[0]?.label).toBe("0s");
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

describe("buildGanttRowSegments", () => {
  it("groups items by task id in flat node order", () => {
    const flatNodes: Array<GridTask> = [
      { depth: 0, id: "t1", is_mapped: false, label: "a" },
      { depth: 0, id: "t2", is_mapped: false, label: "b" },
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
  it("removes execution selectors when linking an aggregate group", () => {
    expect(
      getGanttSegmentTo({
        dagId: "dag",
        item: { isGroup: true, taskId: "body", x: [0, 1], y: "body" },
        maxTryByKey: new Map(),
        pathname: "/dags/dag/runs/run",
        runId: "run",
        searchParams: new URLSearchParams("region_id=stale&region_index=2&try_number=4&view=graph"),
      }),
    ).toEqual({
      pathname: "/dags/dag/runs/run/tasks/group/body",
      search: "view=graph",
    });
  });

  it.each([
    {
      name: "pins try_number only on the older try of a loop region",
      regionId: "00000000-0000-0000-0000-000000000123",
      search: {
        "execution-0-1": "region_id=00000000-0000-0000-0000-000000000123&region_index=0&try_number=1",
        "execution-0-2": "region_id=00000000-0000-0000-0000-000000000123&region_index=0",
        "execution-2-1": "region_id=00000000-0000-0000-0000-000000000123&region_index=2",
      },
    },
    {
      name: "links the sentinel region without region or try selectors for the latest try",
      regionId: "00000000-0000-0000-0000-000000000000",
      search: { "execution-0-1": "try_number=1", "execution-0-2": "", "execution-2-1": "" },
    },
  ])("$name", ({ regionId, search }) => {
    const allTries = [
      { index: 0, tryNumber: 1 },
      { index: 0, tryNumber: 2 },
      { index: 2, tryNumber: 1 },
    ].map(({ index, tryNumber }) => ({
      end_date: "2024-03-14T10:05:00Z",
      id: `execution-${index}-${tryNumber}`,
      map_index: -1,
      queued_dttm: null,
      region_id: regionId,
      region_index: index,
      scheduled_dttm: null,
      start_date: "2024-03-14T10:00:00Z",
      state: "success" as const,
      task_display_name: "work",
      task_id: "body.work",
      try_number: tryNumber,
    }));
    const items = transformGanttData({
      allTries,
      flatNodes: [{ depth: 0, id: "body.work", is_mapped: false, label: "work" }],
      gridSummaries: [],
    });
    const maxTryByKey = buildMaxTryByKey(items);

    expect(
      Object.fromEntries(
        items.map((item) => [
          item.taskInstanceId,
          getGanttSegmentTo({
            dagId: "dag",
            item,
            maxTryByKey,
            pathname: "/dags/dag/runs/run",
            runId: "run",
            searchParams: new URLSearchParams("region_id=stale&region_index=99"),
          })?.search,
        ]),
      ),
    ).toEqual(search);
  });

  it("returns no segments when the try has no schedule, queue, or start time", () => {
    const result = transformGanttData({
      allTries: [
        {
          end_date: null,
          id: "try-1",
          is_mapped: false,
          map_index: -1,
          queued_dttm: null,
          region_id: "00000000-0000-0000-0000-000000000000",
          region_index: -1,
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
          id: "try-2",
          is_mapped: false,
          map_index: -1,
          queued_dttm: null,
          region_id: "00000000-0000-0000-0000-000000000000",
          region_index: -1,
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
          id: "try-3",
          is_mapped: false,
          map_index: -1,
          queued_dttm: null,
          region_id: "00000000-0000-0000-0000-000000000000",
          region_index: -1,
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
          id: "try-4",
          is_mapped: false,
          map_index: -1,
          queued_dttm: "2024-03-14T09:59:00+00:00",
          region_id: "00000000-0000-0000-0000-000000000000",
          region_index: -1,
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

  it("carries the task's actual start and end on every segment of the try", () => {
    const result = transformGanttData({
      allTries: [
        {
          end_date: "2024-03-14T10:05:00+00:00",
          id: "try-5",
          is_mapped: false,
          map_index: -1,
          queued_dttm: "2024-03-14T09:59:00+00:00",
          region_id: "00000000-0000-0000-0000-000000000000",
          region_index: -1,
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
    // The scheduled, queued, and execution bars all report the task's real start_date/end_date
    // (the raw API strings) so the tooltip is consistent no matter which segment is hovered
    // (regression from #68174).
    for (const segment of result) {
      expect(segment.start_when).toBe("2024-03-14T10:00:00+00:00");
      expect(segment.end_when).toBe("2024-03-14T10:05:00+00:00");
    }
  });

  it("uses the current time as end_when on every segment while the task is still running", () => {
    const now = new Date("2024-03-14T10:30:00.000Z");

    vi.useFakeTimers();
    vi.setSystemTime(now);

    try {
      const result = transformGanttData({
        allTries: [
          {
            end_date: null,
            id: "running-try",
            is_mapped: false,
            map_index: -1,
            queued_dttm: "2024-03-14T09:59:00+00:00",
            region_id: "00000000-0000-0000-0000-000000000000",
            region_index: -1,
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

      // Queued + execution bars, both reporting the same "now" end so the tooltip is consistent.
      expect(result.length).toBeGreaterThan(0);
      for (const segment of result) {
        expect(segment.end_when).toBe(now.toISOString());
      }
    } finally {
      vi.useRealTimers();
    }
  });

  it("produces 2 segments when only queued_dttm is present", () => {
    const result = transformGanttData({
      allTries: [
        {
          end_date: "2024-03-14T10:05:00+00:00",
          id: "try-6",
          is_mapped: false,
          map_index: -1,
          queued_dttm: "2024-03-14T09:59:00+00:00",
          region_id: "00000000-0000-0000-0000-000000000000",
          region_index: -1,
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
          id: "try-7",
          is_mapped: false,
          map_index: -1,
          queued_dttm: null,
          region_id: "00000000-0000-0000-0000-000000000000",
          region_index: -1,
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
          id: "try-8",
          is_mapped: false,
          map_index: -1,
          queued_dttm: null,
          region_id: "00000000-0000-0000-0000-000000000000",
          region_index: -1,
          scheduled_dttm: null,
          start_date: "2024-03-14T10:00:00+00:00",
          state: "failed",
          task_display_name: "task_1",
          task_id: "task_1",
          try_number: 2,
        },
        {
          end_date: "2024-03-14T09:55:00+00:00",
          id: "try-9",
          is_mapped: false,
          map_index: -1,
          queued_dttm: null,
          region_id: "00000000-0000-0000-0000-000000000000",
          region_index: -1,
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
