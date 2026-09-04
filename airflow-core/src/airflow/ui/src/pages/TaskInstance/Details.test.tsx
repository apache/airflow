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
import type * as ReactRouterDom from "react-router-dom";
import { beforeEach, describe, expect, it, vi } from "vitest";

import type { TaskInstanceResponse, TaskInstanceRetryDetails } from "openapi/requests/types.gen";
import type * as Utils from "src/utils";
import { Wrapper } from "src/utils/Wrapper";

import { Details } from "./Details";

const queryMocks = vi.hoisted(() => ({
  useMappedTaskInstance: vi.fn(),
  useRetryDetails: vi.fn(),
  useTryDetails: vi.fn(),
}));

let taskState: TaskInstanceResponse["state"] = "up_for_retry";
let selectedTryNumber: string | null = null;

vi.mock("openapi/queries", () => ({
  useTaskInstanceServiceGetMappedTaskInstance: queryMocks.useMappedTaskInstance,
  useTaskInstanceServiceGetMappedTaskInstanceRetryDetails: queryMocks.useRetryDetails,
  useTaskInstanceServiceGetTaskInstanceTryDetails: queryMocks.useTryDetails,
}));
vi.mock("react-i18next", () => ({
  // eslint-disable-next-line id-length
  useTranslation: () => ({ t: (key: string) => key }),
}));
vi.mock("react-router-dom", async (importOriginal) => ({
  ...(await importOriginal<typeof ReactRouterDom>()),
  useParams: () => ({ dagId: "test_dag", mapIndex: "-1", runId: "test_run", taskId: "test_task" }),
  useSearchParams: () => {
    const searchParams = new URLSearchParams();

    if (selectedTryNumber !== null) {
      searchParams.set("try_number", selectedTryNumber);
    }

    return [searchParams, vi.fn()];
  },
}));
vi.mock("src/components/TaskTrySelect", () => ({ TaskTrySelect: () => null }));
vi.mock("src/hooks/useShowTeam", () => ({ useShowTeam: () => false }));
vi.mock("src/utils", async (importOriginal) => ({
  ...(await importOriginal<typeof Utils>()),
  useAutoRefresh: () => false,
}));
vi.mock("./BlockingDeps", () => ({ BlockingDeps: () => null }));
vi.mock("./ExtraLinks", () => ({ ExtraLinks: () => null }));
vi.mock("./RetryDetails", () => ({ RetryDetails: () => <div>Retry details panel</div> }));
vi.mock("./TriggererInfo", () => ({ TriggererInfo: () => null }));

const retryDetails: TaskInstanceRetryDetails = {
  backoff_delay_seconds: 600,
  configured_delay_seconds: 600,
  delay_seconds: 647,
  eligible_at: "2026-08-27T16:31:42Z",
  is_capped: false,
  jitter_seconds: 47,
  maximum_delay_seconds: 1800,
  reason: null,
  source: "task_configuration",
};

const buildTaskInstance = (): TaskInstanceResponse =>
  ({
    dag_id: "test_dag",
    dag_run_id: "test_run",
    map_index: -1,
    state: taskState,
    task_id: "test_task",
    try_number: 2,
  }) as TaskInstanceResponse;

describe("TaskInstance Details", () => {
  beforeEach(() => {
    taskState = "up_for_retry";
    selectedTryNumber = null;
    queryMocks.useMappedTaskInstance.mockImplementation(() => ({ data: buildTaskInstance() }));
    queryMocks.useTryDetails.mockImplementation(() => ({ data: buildTaskInstance() }));
    queryMocks.useRetryDetails.mockImplementation(
      (_params: unknown, _queryKey: unknown, options?: { enabled?: boolean }) => ({
        data: options?.enabled ? retryDetails : undefined,
      }),
    );
  });

  it("shows retry details for the current attempt waiting to retry", () => {
    render(<Details />, { wrapper: Wrapper });

    expect(screen.getByText("Retry details panel")).toBeInTheDocument();
    expect(queryMocks.useRetryDetails).toHaveBeenCalledWith(
      {
        dagId: "test_dag",
        dagRunId: "test_run",
        mapIndex: -1,
        taskId: "test_task",
      },
      undefined,
      expect.objectContaining({ enabled: true }),
    );
  });

  it("does not request retry details for a historical attempt", () => {
    selectedTryNumber = "1";

    render(<Details />, { wrapper: Wrapper });

    expect(screen.queryByText("Retry details panel")).not.toBeInTheDocument();
    expect(queryMocks.useRetryDetails).toHaveBeenCalledWith(
      expect.anything(),
      undefined,
      expect.objectContaining({ enabled: false }),
    );
  });

  it("does not request retry details for a task that is not waiting to retry", () => {
    taskState = "running";

    render(<Details />, { wrapper: Wrapper });

    expect(screen.queryByText("Retry details panel")).not.toBeInTheDocument();
    expect(queryMocks.useRetryDetails).toHaveBeenCalledWith(
      expect.anything(),
      undefined,
      expect.objectContaining({ enabled: false }),
    );
  });
});
