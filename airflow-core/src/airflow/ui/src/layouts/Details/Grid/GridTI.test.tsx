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
import "@testing-library/jest-dom/vitest";
import { render, screen } from "@testing-library/react";
import { MemoryRouter, Route, Routes } from "react-router-dom";
import { beforeEach, describe, expect, it, vi } from "vitest";

import type { LightGridTaskInstanceSummary } from "openapi/requests/types.gen";

import { TimezoneProvider } from "src/context/timezone";
import { BaseWrapper } from "src/utils/Wrapper";

import { GridTI } from "./GridTI";
import { NOTE_GRADIENT, SELECTED_TASK_OUTLINE_COLOR } from "./constants";

const colorModeMock = vi.fn<() => { colorMode: "dark" | "light" | undefined }>();

vi.mock("src/context/colorMode", () => ({
  useColorMode: () => colorModeMock(),
}));

const taskInstance: LightGridTaskInstanceSummary = {
  child_states: null,
  dag_version_number: 1,
  max_end_date: null,
  min_start_date: null,
  state: "success",
  task_display_name: "selected_task",
  task_id: "selected_task",
};

const SELECTED_RUN_ID = "manual__2026-04-21T00:00:00+00:00";

const buildStackShadow = (backCardState: string) =>
  `0.5px 0.5px 0 0 var(--chakra-colors-bg), 2.5px 2.5px 0 0 var(--chakra-colors-${backCardState}-solid)`;

type RenderGridTIOptions = {
  readonly hasNote?: boolean;
  readonly instance?: LightGridTaskInstanceSummary;
  readonly isGroup?: boolean;
  readonly isMapped?: boolean;
  readonly runId?: string;
  readonly taskId?: string;
};

const renderGridTI = (
  route: string,
  {
    hasNote,
    instance = taskInstance,
    isGroup,
    isMapped,
    runId = SELECTED_RUN_ID,
    taskId = "selected_task",
  }: RenderGridTIOptions = {},
) =>
  render(
    <BaseWrapper>
      <TimezoneProvider>
        <MemoryRouter initialEntries={[route]}>
          <Routes>
            <Route
              element={
                <GridTI
                  dagId="example_dag"
                  hasNote={hasNote}
                  instance={{ ...instance, task_id: taskId }}
                  isGroup={isGroup}
                  isMapped={isMapped}
                  label={taskId}
                  runId={runId}
                  taskId={taskId}
                />
              }
              path="/dags/:dagId/runs/:runId/tasks/:taskId"
            />
          </Routes>
        </MemoryRouter>
      </TimezoneProvider>
    </BaseWrapper>,
  );

describe("GridTI", () => {
  it.each<{
    childStates: Record<string, number>;
    primaryState: LightGridTaskInstanceSummary["state"];
    secondaryState: string;
  }>([
    { childStates: { skipped: 2, success: 1 }, primaryState: "success", secondaryState: "skipped" },
    {
      childStates: { failed: 1, skipped: 3, success: 2 },
      primaryState: "failed",
      secondaryState: "success",
    },
    {
      childStates: { running: 1, scheduled: 2, success: 3 },
      primaryState: "running",
      secondaryState: "scheduled",
    },
    { childStates: { none: 2, success: 1 }, primaryState: null, secondaryState: "success" },
  ])(
    "shows the highest-priority secondary state when mapped instances have different states",
    ({ childStates, primaryState, secondaryState }) => {
      renderGridTI(`/dags/example_dag/runs/${SELECTED_RUN_ID}/tasks/selected_task`, {
        instance: { ...taskInstance, child_states: childStates, state: primaryState },
        isMapped: true,
      });

      expect(screen.getByTestId("task-state-badge").style.boxShadow).toBe(buildStackShadow(secondaryState));
    },
  );

  it.each<{
    childStates: Record<string, number> | null;
    primaryState: LightGridTaskInstanceSummary["state"];
  }>([
    { childStates: null, primaryState: "success" },
    { childStates: {}, primaryState: "success" },
    { childStates: { success: 3 }, primaryState: "success" },
    { childStates: { skipped: 0, success: 3 }, primaryState: "success" },
    { childStates: { none: 2 }, primaryState: null },
  ])(
    "colors the back card with the primary state when mapped instances share one state",
    ({ childStates, primaryState }) => {
      renderGridTI(`/dags/example_dag/runs/${SELECTED_RUN_ID}/tasks/selected_task`, {
        instance: { ...taskInstance, child_states: childStates, state: primaryState },
        isMapped: true,
      });

      expect(screen.getByTestId("task-state-badge").style.boxShadow).toBe(
        buildStackShadow(primaryState ?? "none"),
      );
    },
  );

  it.each<{
    backCardState: string;
    childStates: Record<string, number>;
    primaryState: LightGridTaskInstanceSummary["state"];
  }>([
    { backCardState: "skipped", childStates: { skipped: 2, success: 1 }, primaryState: "success" },
    { backCardState: "success", childStates: { success: 3 }, primaryState: "success" },
    {
      backCardState: "upstream_failed",
      childStates: { failed: 1, success: 1, upstream_failed: 2 },
      primaryState: "failed",
    },
    { backCardState: "success", childStates: { failed: 2, success: 3 }, primaryState: "failed" },
  ])("stacks task group cells like mapped tasks", ({ backCardState, childStates, primaryState }) => {
    renderGridTI(`/dags/example_dag/runs/${SELECTED_RUN_ID}/tasks/selected_task`, {
      instance: { ...taskInstance, child_states: childStates, state: primaryState },
      isGroup: true,
    });

    expect(screen.getByTestId("task-state-badge").style.boxShadow).toBe(buildStackShadow(backCardState));
  });

  it("shows a single card for unmapped tasks", () => {
    renderGridTI(`/dags/example_dag/runs/${SELECTED_RUN_ID}/tasks/selected_task`, {
      instance: { ...taskInstance, child_states: { skipped: 2, success: 1 } },
      isMapped: false,
    });

    expect(screen.getByTestId("task-state-badge").style.boxShadow).toBe("");
  });

  it.each([true, false])("preserves note markers and selection highlighting on task cells", (isMapped) => {
    renderGridTI(`/dags/example_dag/runs/${SELECTED_RUN_ID}/tasks/selected_task`, {
      hasNote: true,
      instance: { ...taskInstance, child_states: { skipped: 2, success: 1 } },
      isMapped,
    });

    expect(screen.getByTestId("task-state-badge")).toHaveAttribute("data-selected", "true");
    expect(screen.getByTestId("task-state-badge").style.backgroundImage).toBe(NOTE_GRADIENT);
    expect(screen.getByTestId("task-state-badge").style.boxShadow).toBe(
      isMapped ? buildStackShadow("skipped") : "",
    );
  });

  it("drops another execution's exact region when following an aggregate cell", () => {
    renderGridTI(
      `/dags/example_dag/runs/${SELECTED_RUN_ID}/tasks/selected_task?region_id=11111111-1111-4111-8111-111111111111&region_index=3&try_number=2&log_level=error`,
      { taskId: "other_task" },
    );
    const link = screen.getByRole("link");

    expect(link).toHaveAttribute("href", expect.stringContaining("log_level=error"));
    expect(link).not.toHaveAttribute("href", expect.stringContaining("region_"));
    expect(link).not.toHaveAttribute("href", expect.stringContaining("try_number="));
  });
  beforeEach(() => {
    colorModeMock.mockReturnValue({ colorMode: "light" });
  });

  it("marks the selected task square", () => {
    renderGridTI(`/dags/example_dag/runs/${SELECTED_RUN_ID}/tasks/selected_task`);

    expect(screen.getByTestId("task-state-badge")).toHaveAttribute("data-selected", "true");
    expect(screen.getByTestId("task-state-badge").closest("[data-task-id='selected_task']")).toHaveAttribute(
      "data-selected",
      "true",
    );
  });

  it("uses a lighter outline for the selected task square in dark mode", () => {
    expect(SELECTED_TASK_OUTLINE_COLOR.dark).toBe("brand.contrast");
  });

  it("does not mark another task square as selected", () => {
    renderGridTI(`/dags/example_dag/runs/${SELECTED_RUN_ID}/tasks/selected_task`, {
      taskId: "other_task",
    });

    expect(screen.getByTestId("task-state-badge")).not.toHaveAttribute("data-selected");
    expect(screen.getByTestId("task-state-badge").closest("[data-task-id='other_task']")).toHaveAttribute(
      "data-selected",
      "false",
    );
  });

  it("keeps the task row selected without marking the same task square in another Dag run as selected", () => {
    renderGridTI(`/dags/example_dag/runs/${SELECTED_RUN_ID}/tasks/selected_task`, { runId: "other_run" });

    expect(screen.getByTestId("task-state-badge")).not.toHaveAttribute("data-selected");
    expect(screen.getByTestId("task-state-badge").closest("[data-task-id='selected_task']")).toHaveAttribute(
      "data-selected",
      "true",
    );
  });

  it("links to the task overview when the run has no task instance", () => {
    renderGridTI(`/dags/example_dag/runs/${SELECTED_RUN_ID}/tasks/selected_task`, {
      instance: { ...taskInstance, dag_version_number: null },
      taskId: "missing_task",
    });

    expect(screen.getByTestId(`grid-${SELECTED_RUN_ID}-missing_task`)).toHaveAttribute(
      "href",
      "/dags/example_dag/tasks/missing_task",
    );
  });
});
