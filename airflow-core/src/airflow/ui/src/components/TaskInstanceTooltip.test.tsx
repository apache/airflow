/* eslint-disable unicorn/no-null */

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

import type { LightGridTaskInstanceSummary } from "openapi/requests/types.gen";
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
    expect(screen.getByText(/endDate/iu)).toBeInTheDocument();
    expect(screen.getByText(/duration/iu)).toBeInTheDocument();
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
});
