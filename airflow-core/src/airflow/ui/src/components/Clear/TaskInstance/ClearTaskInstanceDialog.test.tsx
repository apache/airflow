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
import { fireEvent, render, screen, waitFor, within } from "@testing-library/react";
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
  region_id: "00000000-0000-0000-0000-000000000000",
  region_index: -1,
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

const loopPass = (id: string, regionIndex: number): TaskInstanceResponse => ({
  ...taskInstance,
  id,
  region_id: "11111111-1111-4111-8111-111111111111",
  region_index: regionIndex,
  task_id: "body.work",
});

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
  server.listen({ onUnhandledFrame: "bypass" });
});
afterEach(() => {
  affectedTasks.task_instances = [taskInstance];
  mutateMock.mockClear();
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

  it("clears the kept loop passes by execution id when another pass is unticked", async () => {
    affectedTasks.task_instances = [loopPass("pass-0", 0), loopPass("pass-2", 2)];

    render(<ClearTaskInstanceDialog onClose={vi.fn()} open taskInstance={taskInstance} />, {
      wrapper: Wrapper,
    });

    const rows = await screen.findAllByRole("row");

    fireEvent.click(within(rows[1] as HTMLElement).getByRole("checkbox"));
    fireEvent.click(await screen.findByRole("button", { name: /modal\.confirm/iu }));

    await waitFor(() => expect(mutateMock).toHaveBeenCalledTimes(1));
    const [{ requestBody }] = mutateMock.mock.calls[0] as [
      { requestBody: { task_ids?: unknown; task_instance_ids?: Array<string> } },
    ];

    expect(requestBody.task_instance_ids).toEqual(["pass-2"]);
    expect(requestBody.task_ids).toBeUndefined();
  });

  it("keeps an unticked loop pass excluded when its retry comes back with a new execution id", async () => {
    affectedTasks.task_instances = [loopPass("pass-0", 0), loopPass("pass-2", 2)];

    const dialog = <ClearTaskInstanceDialog onClose={vi.fn()} open taskInstance={taskInstance} />;
    const { rerender } = render(dialog, { wrapper: Wrapper });

    const rows = await screen.findAllByRole("row");

    fireEvent.click(within(rows[1] as HTMLElement).getByRole("checkbox"));

    affectedTasks.task_instances = [loopPass("pass-0-retried", 0), loopPass("pass-2", 2)];
    rerender(dialog);

    fireEvent.click(await screen.findByRole("button", { name: /modal\.confirm/iu }));

    await waitFor(() => expect(mutateMock).toHaveBeenCalledTimes(1));
    const [{ requestBody }] = mutateMock.mock.calls[0] as [
      { requestBody: { task_instance_ids?: Array<string> } },
    ];

    expect(requestBody.task_instance_ids).toEqual(["pass-2"]);
  });

  it.each([
    { dialog: "execution", expected: "execution.clearDownstream", in_loop: true },
    { dialog: "task instance", expected: "dags:runAndTaskActions.options.keepTaskState", in_loop: false },
  ])(
    "opens the $dialog clear dialog for a mapped task instance with in_loop=$in_loop",
    async ({ expected, in_loop: inLoop }) => {
      const mapped = {
        ...taskInstance,
        in_loop: inLoop,
        map_index: 1,
        region_id: "11111111-1111-4111-8111-111111111111",
        region_index: 1,
      };

      render(<ClearTaskInstanceDialog onClose={vi.fn()} open taskInstance={mapped} />, {
        wrapper: Wrapper,
      });

      expect(await screen.findByRole("checkbox", { name: new RegExp(expected, "u") })).toBeVisible();
      expect(
        screen.queryByRole("button", { name: inLoop ? /modal\.confirm/iu : /execution\.clearSelected/iu }),
      ).not.toBeInTheDocument();
    },
  );
});
