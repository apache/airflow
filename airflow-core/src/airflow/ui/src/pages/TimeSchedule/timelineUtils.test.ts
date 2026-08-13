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

import type { DAGRunResponse } from "openapi/requests/types.gen";

import { aggregateTimelineItems, buildTimelineItems, getAggregationWindowMinutes } from "./timelineUtils";

const buildDagRun = (
  dagRun: Pick<DAGRunResponse, "duration" | "end_date" | "start_date">,
): DAGRunResponse => ({
  bundle_version: null,
  conf: null,
  dag_display_name: "example_dag",
  dag_id: "example_dag",
  dag_run_id: "run",
  dag_versions: [],
  data_interval_end: null,
  data_interval_start: null,
  duration: dagRun.duration,
  end_date: dagRun.end_date,
  last_scheduling_decision: null,
  logical_date: null,
  note: null,
  partition_date: null,
  partition_key: null,
  queued_at: null,
  run_after: "2024-01-01T00:00:00Z",
  run_type: "scheduled",
  start_date: dagRun.start_date,
  state: "success",
  triggered_by: null,
  triggering_user_name: null,
});

describe("timeline aggregation", () => {
  it("uses the zoom scale as the aggregation window", () => {
    expect(getAggregationWindowMinutes(60)).toBe(60);
    expect(getAggregationWindowMinutes(30)).toBe(30);
    expect(getAggregationWindowMinutes(20)).toBe(20);
    expect(getAggregationWindowMinutes(10)).toBe(10);
    expect(getAggregationWindowMinutes(5)).toBe(5);
    expect(getAggregationWindowMinutes(1)).toBe(1);
  });

  it("aggregates start and end times according to the selected mode", () => {
    const items = buildTimelineItems({
      dagRuns: [
        buildDagRun({
          duration: 60,
          end_date: "2024-01-01T00:01:05Z",
          start_date: "2024-01-01T00:00:05Z",
        }),
        buildDagRun({
          duration: 120,
          end_date: "2024-01-02T00:02:55Z",
          start_date: "2024-01-02T00:00:55Z",
        }),
      ],
      dagRunTimeouts: new Map(),
      includeAllDags: false,
      scheduledDags: [],
    });
    const [mean] = aggregateTimelineItems({
      aggregationMode: "mean",
      items,
      selectedTimezone: "UTC",
      timeScale: 60,
      viewMode: "day",
    });
    const [max] = aggregateTimelineItems({
      aggregationMode: "max",
      items,
      selectedTimezone: "UTC",
      timeScale: 60,
      viewMode: "day",
    });
    const [min] = aggregateTimelineItems({
      aggregationMode: "min",
      items,
      selectedTimezone: "UTC",
      timeScale: 60,
      viewMode: "day",
    });

    expect(mean).toMatchObject({
      endDate: "2024-01-01T00:02:00.000Z",
      runCount: 2,
      startDate: "2024-01-01T00:00:30.000Z",
    });
    expect(max).toMatchObject({
      endDate: "2024-01-01T00:02:55.000Z",
      startDate: "2024-01-01T00:00:05.000Z",
    });
    expect(min).toMatchObject({
      endDate: "2024-01-01T00:01:05.000Z",
      startDate: "2024-01-01T00:00:05.000Z",
    });
  });

  it("keeps different Dag run states in separate aggregates", () => {
    const items = buildTimelineItems({
      dagRuns: [
        buildDagRun({
          duration: 60,
          end_date: "2024-01-01T00:01:00Z",
          start_date: "2024-01-01T00:00:00Z",
        }),
        {
          ...buildDagRun({
            duration: 60,
            end_date: "2024-01-02T00:01:00Z",
            start_date: "2024-01-02T00:00:00Z",
          }),
          dag_run_id: "failed-run",
          state: "failed",
        },
      ],
      dagRunTimeouts: new Map(),
      includeAllDags: false,
      scheduledDags: [],
    });

    const aggregated = aggregateTimelineItems({
      aggregationMode: "mean",
      items,
      selectedTimezone: "UTC",
      timeScale: 60,
      viewMode: "day",
    });

    expect(aggregated.map((item) => item.state)).toEqual(["success", "failed"]);
  });
});
