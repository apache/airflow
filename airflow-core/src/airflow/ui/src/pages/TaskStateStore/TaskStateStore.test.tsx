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
import { render, waitFor } from "@testing-library/react";
import { MemoryRouter, Route, Routes } from "react-router-dom";
import { afterEach, expect, it, vi } from "vitest";

import { TaskInstanceService, TaskStateStoreService } from "openapi/requests";

import { TimezoneProvider } from "src/context/timezone";
import type * as Utils from "src/utils";
import { BaseWrapper } from "src/utils/Wrapper";

import { TaskStateStore } from "./TaskStateStore";

vi.mock("src/router", () => ({ taskInstanceRoutes: [] }));
vi.mock("src/utils", async (importOriginal) => ({
  ...(await importOriginal<typeof Utils>()),
  useAutoRefresh: () => false,
}));
vi.mock("src/queries/useConfig", () => ({ useConfig: () => false }));

afterEach(() => vi.restoreAllMocks());

it("lists state for the exact loop coordinate, retaining the public unmapped index", async () => {
  const fetch = vi
    .spyOn(TaskStateStoreService, "listTaskStateStore")
    .mockResolvedValue({ task_state_store: [], total_entries: 0 });
  const task = vi.spyOn(TaskInstanceService, "getMappedTaskInstance").mockResolvedValue({
    dag_display_name: "dag",
    dag_id: "dag",
    dag_run_id: "run",
    dag_version: null,
    duration: null,
    end_date: null,
    executor: null,
    executor_config: "{}",
    hostname: null,
    id: "execution",
    logical_date: null,
    map_index: -1,
    max_tries: 0,
    note: null,
    operator: null,
    operator_name: null,
    pid: null,
    pool: "default_pool",
    pool_slots: 1,
    priority_weight: null,
    queue: null,
    queued_when: null,
    region_id: "11111111-1111-1111-1111-111111111111",
    region_index: 3,
    rendered_map_index: null,
    run_after: "2026-01-01T00:00:00Z",
    scheduled_when: null,
    start_date: null,
    state: "success",
    task_display_name: "member",
    task_id: "member",
    trigger: null,
    triggerer_job: null,
    try_number: 1,
    unixname: null,
  });

  render(
    <BaseWrapper>
      <MemoryRouter
        initialEntries={[
          "/dags/dag/runs/run/tasks/member/task-state-store?region_id=11111111-1111-1111-1111-111111111111&region_index=3",
        ]}
      >
        <TimezoneProvider>
          <Routes>
            <Route
              element={<TaskStateStore />}
              path="/dags/:dagId/runs/:runId/tasks/:taskId/task-state-store"
            />
          </Routes>
        </TimezoneProvider>
      </MemoryRouter>
    </BaseWrapper>,
  );

  const coordinates = {
    dagId: "dag",
    dagRunId: "run",
    mapIndex: -1,
    regionId: "11111111-1111-1111-1111-111111111111",
    regionIndex: 3,
    taskId: "member",
  };

  await waitFor(() => expect(fetch).toHaveBeenCalledWith(expect.objectContaining(coordinates)));
  expect(task).toHaveBeenCalledWith(coordinates);
});
