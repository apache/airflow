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

import { handlers } from "src/mocks/handlers";
import { Wrapper } from "src/utils/Wrapper";

import ClearTaskInstanceDialog from "./ClearTaskInstanceDialog";

const DAG_ID = "test_dag";
const DAG_RUN_ID = "run_1";
const TASK_ID = "task_1";

const taskInstance = {
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
  ignore_upstream_deps: false,
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
  state: "skipped",
  task_display_name: "task_1",
  task_id: TASK_ID,
  trigger: null,
  triggerer_job: null,
  try_number: 1,
  unixname: null,
} as unknown as TaskInstanceResponse;

const mutateMock = vi.fn();

vi.mock("src/queries/useClearTaskInstances", () => ({
  useClearTaskInstances: () => ({ isPending: false, mutate: mutateMock }),
}));

let server: SetupServer;

beforeAll(() => {
  server = setupServer(
    ...handlers,
    http.get(`/api/v2/dags/${DAG_ID}/details`, () => HttpResponse.json({})),
    http.get(`/api/v2/dags/${DAG_ID}/dagRuns/${DAG_RUN_ID}`, () => HttpResponse.json({ dag_versions: [] })),
    http.post(`/api/v2/dags/${DAG_ID}/clearTaskInstances`, () =>
      HttpResponse.json({ task_instances: [taskInstance], total_entries: 1 }),
    ),
  );
  server.listen({ onUnhandledRequest: "bypass" });
});
afterEach(() => {
  mutateMock.mockClear();
  server.resetHandlers();
  localStorage.clear();
});
afterAll(() => server.close());

// Unlike ClearTaskInstanceDialog.test.tsx, the dry run here answers over the network
// rather than same-tick, so the confirmation dialog has to survive until its response
// lands — the condition under which it used to be dismissed with the clear unsent.
describe("ClearTaskInstanceDialog confirmation hand-off", () => {
  it("still sends the clear when the confirmation dry run resolves asynchronously", async () => {
    render(<ClearTaskInstanceDialog onClose={vi.fn()} open taskInstance={taskInstance} />, {
      wrapper: Wrapper,
    });

    const confirmButton = await screen.findByRole("button", { name: /modal\.confirm/iu });

    await waitFor(() => expect(confirmButton).not.toBeDisabled());
    fireEvent.click(confirmButton);

    await waitFor(() => expect(mutateMock).toHaveBeenCalled());

    const [{ requestBody }] = mutateMock.mock.calls[0] as [{ requestBody: { dry_run: boolean } }];

    expect(requestBody.dry_run).toBe(false);
  });
});
