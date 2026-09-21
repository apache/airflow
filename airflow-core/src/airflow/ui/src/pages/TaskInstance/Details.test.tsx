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
import { describe, expect, it, vi } from "vitest";

import type { TaskInstanceHistoryResponse, TaskInstanceResponse } from "openapi/requests/types.gen";

import i18n from "src/i18n/config";
import { Wrapper } from "src/utils/Wrapper";

import { Details } from "./Details";

// Sibling panels each fetch their own data and are unrelated to the state-reason
// banner and row under test.
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

describe("Details state reason", () => {
  it("does not render the banner when there is no reason", () => {
    renderDetails(buildTaskInstance({ state_reason: null }));

    expect(screen.queryByTestId("state-reason-alert")).not.toBeInTheDocument();
    expect(screen.queryByText(i18n.t("common:taskInstance.stateReason"))).not.toBeInTheDocument();
  });

  it.each([
    { state: "failed", titleKey: "failed" },
    { state: "up_for_retry", titleKey: "upForRetry" },
  ] as const)("titles the banner for a $state task", ({ state, titleKey }) => {
    renderDetails(buildTaskInstance({ max_tries: 2, state, state_reason: "auth error", try_number: 3 }));

    expect(screen.getByTestId("state-reason-alert")).toHaveTextContent(
      i18n.t(`common:taskInstance.stateReasonSummary.${titleKey}`, { totalTries: 3, tryNumber: 3 }),
    );
  });

  // Chakra encodes `status` in a generated class rather than a DOM attribute, so the error/warning
  // distinction can only be pinned as "the two states do not render identically".
  it("styles a failed banner differently from an up_for_retry one", () => {
    const { unmount } = renderDetails(buildTaskInstance({ state: "failed", state_reason: "auth error" }));
    const failedClass = screen.getByTestId("state-reason-alert").className;

    unmount();
    renderDetails(buildTaskInstance({ state: "up_for_retry", state_reason: "auth error" }));

    expect(screen.getByTestId("state-reason-alert").className).not.toBe(failedClass);
  });

  // The reason is only cleared once the task next reaches RUNNING, so a cleared task keeps a
  // reason describing the previous attempt. Gating on state is what stops it being shown.
  it.each(["queued", "running", "success", null] as const)(
    "does not render the banner for a %s task that still carries a reason",
    (state) => {
      renderDetails(buildTaskInstance({ state, state_reason: "auth error, do not retry" }));

      expect(screen.queryByTestId("state-reason-alert")).not.toBeInTheDocument();
    },
  );

  it("titles the banner with the try counts so it is distinct from the per-try row", () => {
    renderDetails(
      buildTaskInstance({ max_tries: 2, state: "failed", state_reason: "rate limit", try_number: 3 }),
    );

    expect(screen.getByTestId("state-reason-alert")).toHaveTextContent(
      i18n.t("common:taskInstance.stateReasonSummary.failed", { totalTries: 3, tryNumber: 3 }),
    );
  });

  it("shows the selected try's reason in the table while the banner keeps the latest try's", () => {
    renderDetails(buildTaskInstance({ state_reason: "latest try: rate limit" }), {
      state_reason: "older try: auth error",
    });

    expect(screen.getByTestId("state-reason-alert")).toHaveTextContent("latest try: rate limit");
    expect(screen.getByText("older try: auth error")).toBeInTheDocument();
    expect(screen.getByText(i18n.t("common:taskInstance.stateReason"))).toBeInTheDocument();
  });
});
