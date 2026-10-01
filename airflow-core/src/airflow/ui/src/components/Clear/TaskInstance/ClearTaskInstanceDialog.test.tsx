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
import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import { http, HttpResponse } from "msw";
import { setupServer, type SetupServer } from "msw/node";
import { afterAll, afterEach, beforeAll, describe, expect, it, vi } from "vitest";

import type { TaskInstanceResponse } from "openapi/requests/types.gen";

import { CLEAR_KEEP_TASK_STATE_KEY } from "src/constants/localStorage";
import { handlers } from "src/mocks/handlers";
import { Wrapper } from "src/utils/Wrapper";

import ClearTaskInstanceDialog from "./ClearTaskInstanceDialog";

const DAG_ID = "test_dag";
const DAG_RUN_ID = "run_1";
const TASK_ID = "task_1";

const taskInstance: TaskInstanceResponse = {
  dag_display_name: "Test DAG",
  dag_id: DAG_ID,
  dag_run_id: DAG_RUN_ID,
  dag_version: null,
  duration: null,
  end_date: null,
  executor: null,
  executor_config: "{}",
  hostname: null,
  id: "test_task_instance",
  logical_date: "2025-01-01T00:00:00Z",
  map_index: -1,
  max_tries: 0,
  note: null,
  operator: "EmptyOperator",
  operator_name: "EmptyOperator",
  pid: null,
  pool: "default_pool",
  pool_slots: 1,
  priority_weight: null,
  queue: null,
  queued_when: null,
  rendered_fields: undefined,
  rendered_map_index: null,
  run_after: "2025-01-01T00:00:00Z",
  scheduled_when: null,
  start_date: null,
  state: "success",
  task_display_name: "task_1",
  task_id: TASK_ID,
  trigger: null,
  triggerer_job: null,
  try_number: 1,
  unixname: null,
};

const affectedTasks = { task_instances: [taskInstance], total_entries: 1 };

const mutateMock = vi.fn();

vi.mock("src/queries/useClearTaskInstances", () => ({
  useClearTaskInstances: () => ({ isPending: false, mutate: mutateMock }),
}));

// Mocked directly (rather than via MSW) because it backs both the affected-tasks
// list in the main dialog and the confirmation dialog's running-task gate; a
// same-tick response for both keeps the gate from blocking on a real fetch.
vi.mock("src/queries/useClearTaskInstancesDryRun", () => ({
  useClearTaskInstancesDryRun: () => ({ data: affectedTasks, isFetching: false, isPending: false }),
}));

let server: SetupServer;

beforeAll(() => {
  server = setupServer(
    ...handlers,
    http.get(`/api/v2/dags/${DAG_ID}/details`, () => HttpResponse.json({})),
    http.get(`/api/v2/dags/${DAG_ID}/dagRuns/${DAG_RUN_ID}`, () => HttpResponse.json({ dag_versions: [] })),
  );
  server.listen({ onUnhandledRequest: "bypass" });
});
afterEach(() => {
  server.resetHandlers();
  localStorage.clear();
});
afterAll(() => server.close());

describe("ClearTaskInstanceDialog", () => {
  it("seeds the keep-task-state checkbox from the stored default and sends it on confirm", async () => {
    localStorage.setItem(CLEAR_KEEP_TASK_STATE_KEY, JSON.stringify(true));

    render(<ClearTaskInstanceDialog onClose={vi.fn()} open taskInstance={taskInstance} />, {
      wrapper: Wrapper,
    });

    const keepTaskStateCheckbox = screen.getByRole("checkbox", { name: /keepTaskState/iu });

    expect(keepTaskStateCheckbox).toBeChecked();

    const confirmButton = await screen.findByRole("button", { name: /modal\.confirm/iu });

    fireEvent.click(confirmButton);

    await waitFor(() => expect(mutateMock).toHaveBeenCalled());

    const [{ requestBody }] = mutateMock.mock.calls[0] as [{ requestBody: { keep_task_state?: boolean } }];

    expect(requestBody.keep_task_state).toBe(true);
  });
});
