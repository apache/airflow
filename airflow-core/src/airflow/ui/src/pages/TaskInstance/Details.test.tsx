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
import { beforeEach, describe, expect, it, vi } from "vitest";

import type { TaskInstanceHistoryResponse, TaskInstanceResponse } from "openapi/requests/types.gen";

import i18n from "src/i18n/config";
import { Wrapper } from "src/utils/Wrapper";

import commonLocale from "../../../public/i18n/locales/en/common.json";
import { Details } from "./Details";

// Sibling panels each fetch their own data and are unrelated to the row under test.
vi.mock("./BlockingDeps", () => ({ BlockingDeps: () => undefined }));
vi.mock("./ExtraLinks", () => ({ ExtraLinks: () => undefined }));
vi.mock("./TriggererInfo", () => ({ TriggererInfo: () => undefined }));
vi.mock("src/components/DagVersionDetails", () => ({ DagVersionDetails: () => undefined }));
vi.mock("src/components/TaskTrySelect", () => ({ TaskTrySelect: () => undefined }));
vi.mock("src/components/TeamName", () => ({ TeamName: () => undefined }));
vi.mock("src/hooks/useShowTeam", () => ({ useShowTeam: () => false }));

const mockTaskInstance = vi.fn<() => TaskInstanceResponse | undefined>();
const mockTryInstance = vi.fn<() => TaskInstanceHistoryResponse | undefined>();

vi.mock("openapi/queries", async () => {
  const actual = await vi.importActual("openapi/queries");

  return {
    ...actual,
    useTaskInstanceServiceGetMappedTaskInstance: () => ({ data: mockTaskInstance() }),
    useTaskInstanceServiceGetTaskInstanceTryDetails: () => ({ data: mockTryInstance() }),
  };
});

vi.mock("src/utils", async () => {
  const actual = await vi.importActual("src/utils");

  return { ...actual, useAutoRefresh: () => false };
});

const buildTaskInstance = (overrides: Partial<TaskInstanceResponse>): TaskInstanceResponse =>
  ({
    dag_id: "test_dag",
    dag_run_id: "run_1",
    dag_version: null,
    duration: null,
    end_date: null,
    id: "ti-id",
    map_index: -1,
    max_tries: 2,
    note: null,
    operator_name: "PythonOperator",
    rendered_map_index: null,
    start_date: null,
    state: "failed",
    state_reason: null,
    task_display_name: "test_task",
    task_id: "test_task",
    trigger: null,
    triggerer_job: null,
    try_number: 3,
    ...overrides,
  }) as unknown as TaskInstanceResponse;

const renderDetails = (
  taskInstance: TaskInstanceResponse,
  tryInstance: Partial<TaskInstanceHistoryResponse> = {},
) => {
  mockTaskInstance.mockReturnValue(taskInstance);
  mockTryInstance.mockReturnValue({
    ...taskInstance,
    ...tryInstance,
  });

  return render(<Details />, { wrapper: Wrapper });
};

describe("Details state reason row", () => {
  // Without the bundle i18n.t() echoes the key, so the label assertions below would pass blindly.
  beforeEach(() => {
    i18n.addResourceBundle("en", "common", commonLocale, true, true);
  });

  it("does not render the banner when there is no reason", () => {
    renderDetails(buildTaskInstance({ state_reason: null }));

    expect(screen.queryByText(i18n.t("common:taskInstance.stateReason"))).not.toBeInTheDocument();
  });

  // Cleared only once the task next reaches RUNNING, so these states still carry a stale reason.
  it.each(["queued", "running", "success", null] as const)(
    "renders neither the banner nor the row for a %s task that still carries a reason",
    (state) => {
      renderDetails(buildTaskInstance({ state, state_reason: "auth error, do not retry" }));

      expect(screen.queryByText(i18n.t("common:taskInstance.stateReason"))).not.toBeInTheDocument();
      expect(screen.queryByText("auth error, do not retry")).not.toBeInTheDocument();
    },
  );

  it("keeps an earlier failed try's reason while the task is running again", () => {
    renderDetails(buildTaskInstance({ state: "running", state_reason: null }), {
      state: "failed",
      state_reason: "try 1: auth error",
    });

    expect(screen.getByText("try 1: auth error")).toBeInTheDocument();
  });

  it("shows the selected try's reason in the table", () => {
    renderDetails(buildTaskInstance({ state_reason: "latest try: rate limit" }), {
      state_reason: "older try: auth error",
    });

    expect(screen.getByText("older try: auth error")).toBeInTheDocument();
    expect(screen.queryByText("latest try: rate limit")).not.toBeInTheDocument();
    expect(screen.getByText(i18n.t("common:taskInstance.stateReason"))).toBeInTheDocument();
  });
});
