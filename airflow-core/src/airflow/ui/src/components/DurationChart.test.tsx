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
import { render } from "@testing-library/react";
import { Bar } from "react-chartjs-2";
import { describe, expect, it, vi } from "vitest";

import type { GridRunsResponse } from "openapi/requests/types.gen";
import { TimezoneContext } from "src/context/timezone";
import { Wrapper } from "src/utils/Wrapper";

import { DurationChart } from "./DurationChart";

const makeRun = (runAfter: string): GridRunsResponse => ({
  dag_id: "tutorial_dag",
  duration: 60,
  end_date: `${runAfter.slice(0, -1)}:01:00Z`,
  has_missed_deadline: false,
  has_note: false,
  queued_at: runAfter,
  run_after: runAfter,
  run_id: runAfter,
  run_type: "scheduled",
  start_date: runAfter,
  state: "success",
});

// A Dag scheduled twice a day: the two runs land 12 hours apart on the same date.
const entries = [makeRun("2026-08-20T08:30:00Z"), makeRun("2026-08-20T20:30:00Z")];

const renderChart = (selectedTimezone: string) => {
  render(
    <TimezoneContext.Provider value={{ selectedTimezone, setSelectedTimezone: vi.fn() }}>
      <DurationChart entries={entries} kind="Dag Run" />
    </TimezoneContext.Provider>,
    { wrapper: Wrapper },
  );

  return vi.mocked(Bar).mock.calls.at(-1)?.[0].data.labels;
};

describe("DurationChart", () => {
  it.each([
    { expected: ["2026-08-20 08:30:00", "2026-08-20 20:30:00"], timezone: "UTC" },
    { expected: ["2026-08-20 17:30:00", "2026-08-21 05:30:00"], timezone: "Asia/Tokyo" },
    { expected: ["2026-08-20 04:30:00", "2026-08-20 16:30:00"], timezone: "America/New_York" },
  ])("labels each bar in the $timezone timezone", ({ expected, timezone }) => {
    expect(renderChart(timezone)).toEqual(expected);
  });
});
