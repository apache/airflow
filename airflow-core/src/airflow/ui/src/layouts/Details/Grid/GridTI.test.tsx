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
import { MemoryRouter, Route, Routes } from "react-router-dom";
import { beforeEach, describe, expect, it, vi } from "vitest";

import type { LightGridTaskInstanceSummary } from "openapi/requests/types.gen";
import { TimezoneProvider } from "src/context/timezone";
import { BaseWrapper } from "src/utils/Wrapper";

import { GridTI } from "./GridTI";
import { SELECTED_TASK_OUTLINE_COLOR } from "./constants";

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

type RenderGridTIOptions = {
  readonly instance?: LightGridTaskInstanceSummary;
  readonly runId?: string;
  readonly taskId?: string;
};

const renderGridTI = (
  route: string,
  { instance = taskInstance, runId = SELECTED_RUN_ID, taskId = "selected_task" }: RenderGridTIOptions = {},
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
                  instance={{ ...instance, task_id: taskId }}
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
