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
import type { ChartEvent, ActiveElement } from "chart.js";
import dayjs from "dayjs";
import { describe, it, expect, vi } from "vitest";

import type { GanttDataItem } from "./utils";
import { createChartOptions, transformGanttData } from "./utils";

const noop = () => {};

const defaultChartParams = {
  gridColor: "#ccc",
  handleBarClick: noop as (event: ChartEvent, elements: Array<ActiveElement>) => void,
  handleBarHover: noop as (event: ChartEvent, elements: Array<ActiveElement>) => void,
  hoveredId: undefined,
  hoveredItemColor: "#eee",
  labels: ["task_1", "task_2"],
  selectedId: undefined,
  selectedItemColor: "#ddd",
  selectedTimezone: "UTC",
  translate: ((key: string) => key) as ReturnType<typeof vi.fn>,
};

describe("createChartOptions", () => {
  describe("x-axis scale min/max with ISO date strings", () => {
    it("should compute valid min/max for completed tasks with ISO dates", () => {
      const data: Array<GanttDataItem> = [
        {
          state: "success",
          taskId: "task_1",
          x: ["2024-03-14T10:00:00.000Z", "2024-03-14T10:05:00.000Z"],
          y: "task_1",
        },
        {
          state: "success",
          taskId: "task_2",
          x: ["2024-03-14T10:03:00.000Z", "2024-03-14T10:10:00.000Z"],
          y: "task_2",
        },
      ];

      const options = createChartOptions({
        ...defaultChartParams,
        data,
        selectedRun: {
          dag_id: "test_dag",
          duration: 600,
          end_date: "2024-03-14T10:10:00+00:00",
          queued_at: "2024-03-14T09:59:00+00:00",
          run_after: "2024-03-14T10:00:00+00:00",
          run_id: "run_1",
          run_type: "manual",
          start_date: "2024-03-14T10:00:00+00:00",
          state: "success",
        },
      });

      const xScale = options.scales.x;

      expect(xScale.min).toBeTypeOf("number");
      expect(xScale.max).toBeTypeOf("number");
      expect(Number.isNaN(xScale.min)).toBe(false);
      expect(Number.isNaN(xScale.max)).toBe(false);
      // max should be slightly beyond the latest end date (5% padding)
      expect(xScale.max).toBeGreaterThan(new Date("2024-03-14T10:10:00.000Z").getTime());
    });

    it("should compute valid min/max for running tasks", () => {
      const now = dayjs().toISOString();
      const data: Array<GanttDataItem> = [
        {
          state: "success",
          taskId: "task_1",
          x: ["2024-03-14T10:00:00.000Z", "2024-03-14T10:05:00.000Z"],
          y: "task_1",
        },
        {
          state: "running",
          taskId: "task_2",
          x: ["2024-03-14T10:05:00.000Z", now],
          y: "task_2",
        },
      ];

      const options = createChartOptions({
        ...defaultChartParams,
        data,
        selectedRun: {
          dag_id: "test_dag",
          // eslint-disable-next-line unicorn/no-null
          end_date: null,
          duration: 0,
          queued_at: "2024-03-14T09:59:00+00:00",
          run_after: "2024-03-14T10:00:00+00:00",
          run_id: "run_1",
          run_type: "manual",
          start_date: "2024-03-14T10:00:00+00:00",
          state: "running",
        },
      });

      const xScale = options.scales.x;

      expect(xScale.min).toBeTypeOf("number");
      expect(xScale.max).toBeTypeOf("number");
      expect(Number.isNaN(xScale.min)).toBe(false);
      expect(Number.isNaN(xScale.max)).toBe(false);
    });

    it("should handle empty data with running DagRun (fallback to formatted dates)", () => {
      const options = createChartOptions({
        ...defaultChartParams,
        data: [],
        labels: [],
        selectedRun: {
          dag_id: "test_dag",
          duration: 0,
          // eslint-disable-next-line unicorn/no-null
          end_date: null,
          queued_at: "2024-03-14T09:59:00+00:00",
          run_after: "2024-03-14T10:00:00+00:00",
          run_id: "run_1",
          run_type: "manual",
          start_date: "2024-03-14T10:00:00+00:00",
          state: "running",
        },
      });

      const xScale = options.scales.x;

      // With empty data, min/max are formatted date strings (fallback branch)
      expect(xScale.min).toBeTypeOf("string");
      expect(xScale.max).toBeTypeOf("string");
    });
  });
});

describe("transformGanttData", () => {
  it("should skip tasks with null start_date", () => {
    const result = transformGanttData({
      allTries: [
        {
          dag_id: "test",
          // eslint-disable-next-line unicorn/no-null
          end_date: null,
          is_mapped: false,
          map_index: -1,
          // eslint-disable-next-line unicorn/no-null
          start_date: null,
          // eslint-disable-next-line unicorn/no-null
          state: null,
          task_display_name: "task_1",
          task_id: "task_1",
          try_number: 1,
        },
      ],
      flatNodes: [{ id: "task_1", label: "task_1", is_mapped: false }],
      gridSummaries: [],
    });

    expect(result).toHaveLength(0);
  });

  it("should include running tasks with valid start_date and use current time as end", () => {
    const before = dayjs();
    const result = transformGanttData({
      allTries: [
        {
          dag_id: "test",
          // eslint-disable-next-line unicorn/no-null
          end_date: null,
          is_mapped: false,
          map_index: -1,
          start_date: "2024-03-14T10:00:00+00:00",
          state: "running",
          task_display_name: "task_1",
          task_id: "task_1",
          try_number: 1,
        },
      ],
      flatNodes: [{ id: "task_1", label: "task_1", is_mapped: false }],
      gridSummaries: [],
    });

    expect(result).toHaveLength(1);
    expect(result[0]?.state).toBe("running");
    // End time should be approximately now (ISO string)
    const endTime = dayjs(result[0]?.x[1]);

    expect(endTime.valueOf()).toBeGreaterThanOrEqual(before.valueOf());
  });

  it("should skip groups with null min_start_date or max_end_date", () => {
    const result = transformGanttData({
      allTries: [],
      flatNodes: [{ id: "group_1", label: "group_1", is_mapped: false, isGroup: true }],
      gridSummaries: [
        {
          // eslint-disable-next-line unicorn/no-null
          child_states: null,
          // eslint-disable-next-line unicorn/no-null
          max_end_date: null,
          // eslint-disable-next-line unicorn/no-null
          min_start_date: null,
          // eslint-disable-next-line unicorn/no-null
          state: null,
          task_id: "group_1",
        },
      ],
    });

    expect(result).toHaveLength(0);
  });

  it("should produce ISO date strings parseable by dayjs", () => {
    const result = transformGanttData({
      allTries: [
        {
          dag_id: "test",
          end_date: "2024-03-14T10:05:00+00:00",
          is_mapped: false,
          map_index: -1,
          start_date: "2024-03-14T10:00:00+00:00",
          state: "success",
          task_display_name: "task_1",
          task_id: "task_1",
          try_number: 1,
        },
      ],
      flatNodes: [{ id: "task_1", label: "task_1", is_mapped: false }],
      gridSummaries: [],
    });

    expect(result).toHaveLength(1);
    // x values should be valid ISO strings that dayjs can parse without NaN
    const start = dayjs(result[0]?.x[0]);
    const end = dayjs(result[0]?.x[1]);

    expect(start.isValid()).toBe(true);
    expect(end.isValid()).toBe(true);
    expect(Number.isNaN(start.valueOf())).toBe(false);
    expect(Number.isNaN(end.valueOf())).toBe(false);
  });
});
