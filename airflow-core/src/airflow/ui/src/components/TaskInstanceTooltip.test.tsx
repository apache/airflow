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
import "@testing-library/jest-dom";
import { render, screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";

import type { LightGridTaskInstanceSummary, TaskInstanceResponse } from "openapi/requests/types.gen";
import { Wrapper } from "src/utils/Wrapper";

import TaskInstanceTooltip from "./TaskInstanceTooltip";

describe("TaskInstanceTooltip", () => {
  it("renders children directly when both taskInstance and tooltip are undefined", () => {
    render(
      <TaskInstanceTooltip>
        <span data-testid="child">Child content</span>
      </TaskInstanceTooltip>,
      { wrapper: Wrapper },
    );

    expect(screen.getByTestId("child")).toBeInTheDocument();
    // Should not render a tooltip trigger wrapper
    expect(screen.queryByRole("tooltip")).toBeNull();
  });

  it("shows state and dates for LightGridTaskInstanceSummary with both dates", () => {
    const taskInstance: LightGridTaskInstanceSummary = {
      child_states: null,
      max_end_date: "2025-01-01T01:00:00Z",
      min_start_date: "2025-01-01T00:00:00Z",
      state: "success",
      task_display_name: "My Task",
      task_id: "my_task",
    };

    render(
      <TaskInstanceTooltip open taskInstance={taskInstance}>
        <span>trigger</span>
      </TaskInstanceTooltip>,
      { wrapper: Wrapper },
    );

    expect(screen.getByText(/state/iu)).toBeInTheDocument();
    expect(screen.getByText(/startDate/iu)).toBeInTheDocument();
    expect(screen.getByText(/duration/iu)).toBeInTheDocument();
  });

  it("calculates live duration for a running task instead of using stale duration", () => {
    // We pass a stale duration of 50 seconds, but start date is 2 hours ago.
    // The calculated duration should be ~2 hours, not 50 seconds.
    const twoHoursAgo = new Date(Date.now() - 2 * 60 * 60 * 1000).toISOString();
    const taskInstance: TaskInstanceResponse = {
      dag_display_name: "Test DAG",
      dag_id: "test",
      dag_run_id: "test",
      dag_version: null,
      duration: 50.0,
      end_date: null,
      executor: null,
      executor_config: "{}",
      hostname: null,
      id: "test_my_task",
      logical_date: null,
      map_index: 0,
      max_tries: 3,
      note: null,
      operator: "DummyOperator",
      operator_name: "DummyOperator",
      pid: null,
      pool: "default_pool",
      pool_slots: 1,
      priority_weight: null,
      queue: null,
      queued_when: null,
      rendered_fields: undefined,
      rendered_map_index: null,
      run_after: twoHoursAgo,
      scheduled_when: null,
      start_date: twoHoursAgo,
      state: "running",
      task_display_name: "My Task",
      task_id: "my_task",
      trigger: null,
      triggerer_job: null,
      try_number: 2,
      unixname: null,
    };

    render(
      <TaskInstanceTooltip open taskInstance={taskInstance}>
        <span>trigger</span>
      </TaskInstanceTooltip>,
      { wrapper: Wrapper },
    );

    const durationText = screen.getByText(/duration/iu).parentElement?.textContent;

    // The calculated duration should be around 2 hours (e.g., "02:00:00" or similar)
    // It should definitely NOT be "00:00:50.000" which is what `renderDuration(50)` gives
    expect(durationText).not.toContain("00:00:50");
    expect(durationText).toContain("02:00:");
  });

  it("shows only start date when max_end_date is null", () => {
    const taskInstance: LightGridTaskInstanceSummary = {
      child_states: null,
      max_end_date: null,
      min_start_date: "2025-01-01T00:00:00Z",
      state: "running",
      task_display_name: "My Task",
      task_id: "my_task",
    };

    render(
      <TaskInstanceTooltip open taskInstance={taskInstance}>
        <span>trigger</span>
      </TaskInstanceTooltip>,
      { wrapper: Wrapper },
    );

    expect(screen.getByText(/state/iu)).toBeInTheDocument();
    expect(screen.getByText(/startDate/iu)).toBeInTheDocument();
    expect(screen.queryByText(/endDate/iu)).toBeNull();
    expect(screen.queryByText(/duration/iu)).toBeNull();
  });

  it("shows no dates when min_start_date is null", () => {
    const taskInstance: LightGridTaskInstanceSummary = {
      child_states: null,
      max_end_date: null,
      min_start_date: null,
      state: "queued",
      task_display_name: "My Task",
      task_id: "my_task",
    };

    render(
      <TaskInstanceTooltip open taskInstance={taskInstance}>
        <span>trigger</span>
      </TaskInstanceTooltip>,
      { wrapper: Wrapper },
    );

    expect(screen.getByText(/state/iu)).toBeInTheDocument();
    expect(screen.queryByText(/startDate/iu)).toBeNull();
    expect(screen.queryByText(/endDate/iu)).toBeNull();
  });

  it("shows tooltip text when only tooltip prop is provided", () => {
    render(
      <TaskInstanceTooltip open tooltip="My group description">
        <span>trigger</span>
      </TaskInstanceTooltip>,
      { wrapper: Wrapper },
    );

    expect(screen.getByText("My group description")).toBeInTheDocument();
  });

  it("shows both tooltip text and task instance data", () => {
    const taskInstance: LightGridTaskInstanceSummary = {
      child_states: { success: 3 },
      max_end_date: "2025-01-01T02:00:00Z",
      min_start_date: "2025-01-01T00:00:00Z",
      state: "success",
      task_display_name: "Group Task",
      task_id: "group_task",
    };

    render(
      <TaskInstanceTooltip open taskInstance={taskInstance} tooltip="Task Group Info">
        <span>trigger</span>
      </TaskInstanceTooltip>,
      { wrapper: Wrapper },
    );

    expect(screen.getByText("Task Group Info")).toBeInTheDocument();
    expect(screen.getAllByText(/state/iu).length).toBeGreaterThan(0);
    expect(screen.getByText(/startDate/iu)).toBeInTheDocument();
  });

  it("shows run ID when provided explicitly for grid summaries", () => {
    const taskInstance: LightGridTaskInstanceSummary = {
      child_states: null,
      max_end_date: "2025-01-01T02:00:00Z",
      min_start_date: "2025-01-01T00:00:00Z",
      state: "success",
      task_display_name: "My Task",
      task_id: "my_task",
    };

    render(
      <TaskInstanceTooltip open runId="manual__2025-01-01T00:00:00+00:00" taskInstance={taskInstance}>
        <span>trigger</span>
      </TaskInstanceTooltip>,
      { wrapper: Wrapper },
    );

    expect(screen.getByText(/runId/iu)).toBeInTheDocument();
    expect(screen.getByText(/manual__2025-01-01T00:00:00\+00:00/iu)).toBeInTheDocument();
  });
});
