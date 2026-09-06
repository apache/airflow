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

import { buildDayRowLayouts } from "./timelineUtils";
import type { TimelineRow } from "./types";

const row: TimelineRow = {
  dagId: "example_dag",
  isTimeScheduled: true,
  items: [
    {
      dagId: "example_dag",
      dagRunId: "run-1",
      durationMs: 60_000,
      endDate: "2024-01-01T00:01:00Z",
      isPlaceholder: false,
      isPlanned: false,
      isTimeScheduled: true,
      label: "example_dag",
      runCount: 1,
      startDate: "2024-01-01T00:00:00Z",
      state: "success",
    },
    {
      dagId: "example_dag",
      dagRunId: "run-2",
      durationMs: 60_000,
      endDate: "2024-01-01T00:02:00Z",
      isPlaceholder: false,
      isPlanned: false,
      isTimeScheduled: true,
      label: "example_dag",
      runCount: 1,
      startDate: "2024-01-01T00:01:00Z",
      state: "success",
    },
  ],
  label: "example_dag",
};

describe("Time Schedule timeline layout", () => {
  it("assigns overlapping minimum-width bars to separate lanes", () => {
    const [layout] = buildDayRowLayouts({
      rows: [row],
      selectedTimezone: "UTC",
      timelineWidth: 1440,
    });

    expect(layout?.items.map(({ lane }) => lane)).toEqual([0, 1]);
  });
});
