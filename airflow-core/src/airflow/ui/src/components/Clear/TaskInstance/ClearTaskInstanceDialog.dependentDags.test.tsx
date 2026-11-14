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
import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import { http, HttpResponse } from "msw";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import type { TaskInstanceResponse } from "openapi/requests/types.gen";

import { server } from "src/mocks/server";
import { Wrapper } from "src/utils/Wrapper";

import ClearTaskInstanceDialog from "./ClearTaskInstanceDialog";

const DAG_ID = "parent_dag";
const DAG_RUN_ID = "run_1";

const taskInstance: TaskInstanceResponse = {
  dag_display_name: DAG_ID,
  dag_id: DAG_ID,
  dag_run_id: DAG_RUN_ID,
  dag_version: null,
  duration: null,
  end_date: null,
  executor: null,
  executor_config: "{}",
  hostname: null,
  id: "marker",
  in_loop: false,
  logical_date: "2025-01-01T00:00:00Z",
  map_index: -1,
  max_tries: 0,
  note: null,
  operator: "ExternalTaskMarker",
  operator_name: "ExternalTaskMarker",
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
  task_display_name: "mark",
  task_id: "mark",
  trigger: null,
  triggerer_job: null,
  try_number: 1,
  unixname: null,
};

const mutateMock = vi.fn();

vi.mock("src/queries/useClearTaskInstances", () => ({
  useClearTaskInstances: () => ({ isPending: false, mutate: mutateMock }),
}));

vi.mock("src/queries/useClearTaskInstancesDryRun", () => ({
  useClearTaskInstancesDryRun: () => ({
    data: { task_instances: [taskInstance], total_entries: 1 },
    isFetching: false,
    isPending: false,
  }),
  useClearTaskInstancesDryRuns: () => ({
    data: { task_instances: [taskInstance], total_entries: 1 },
    error: null,
    isFetching: false,
  }),
}));

beforeEach(() => {
  server.use(
    http.get(`/api/v2/dags/${DAG_ID}/details`, () => HttpResponse.json({})),
    http.get(`/api/v2/dags/${DAG_ID}/dagRuns/${DAG_RUN_ID}`, () => HttpResponse.json({ dag_versions: [] })),
  );
});
afterEach(() => {
  mutateMock.mockClear();
  localStorage.clear();
});

const confirmAndGetRequestBody = async () => {
  const confirmButton = await screen.findByRole("button", { name: /modal\.confirm/iu });

  await waitFor(() => expect(confirmButton).toBeEnabled());
  fireEvent.click(confirmButton);
  await waitFor(() => expect(mutateMock).toHaveBeenCalled());

  return (mutateMock.mock.calls[0] as [{ requestBody: Record<string, unknown> }])[0].requestBody;
};

describe("ClearTaskInstanceDialog with dependent Dags", () => {
  it("clears dependent Dags along with the downstream tasks by default", async () => {
    render(<ClearTaskInstanceDialog onClose={vi.fn()} open taskInstance={taskInstance} />, {
      wrapper: Wrapper,
    });

    expect(screen.getByRole("checkbox", { name: /includeDependentDags/iu })).toBeChecked();
    expect(await confirmAndGetRequestBody()).toMatchObject({
      include_dependent_dags: true,
      include_downstream: true,
    });
  });

  it("leaves dependent Dags out when the user unticks them", async () => {
    render(<ClearTaskInstanceDialog onClose={vi.fn()} open taskInstance={taskInstance} />, {
      wrapper: Wrapper,
    });

    fireEvent.click(screen.getByRole("checkbox", { name: /includeDependentDags/iu }));

    expect(await confirmAndGetRequestBody()).toMatchObject({
      include_dependent_dags: false,
      include_downstream: true,
    });
  });
});
