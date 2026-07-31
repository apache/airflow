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

import type { TaskInstanceResponse } from "openapi/requests/types.gen";
import { Wrapper } from "src/utils/Wrapper";

import MarkTaskInstanceAsDialog from "./MarkTaskInstanceAsDialog";

vi.mock("src/queries/usePatchTaskInstance", () => ({
  usePatchTaskInstance: () => ({
    isPending: false,
    mutate: vi.fn(),
  }),
}));

vi.mock("src/queries/usePatchTaskInstanceDryRun", () => ({
  usePatchTaskInstanceDryRun: () => ({
    data: {
      task_instances: [],
      total_entries: 0,
    },
    isPending: false,
  }),
}));

const taskInstance: TaskInstanceResponse = {
  dag_display_name: "Test DAG",
  dag_id: "test_dag",
  dag_run_id: "manual__2025-01-01T00:00:00+00:00",
  dag_version: null,
  duration: null,
  end_date: null,
  executor: null,
  executor_config: "{}",
  hostname: null,
  id: "test_task_instance",
  logical_date: "2025-01-01T00:00:00Z",
  map_index: 0,
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
  state: "failed",
  task_display_name: "task_1",
  task_id: "task_1",
  trigger: null,
  triggerer_job: null,
  try_number: 1,
  unixname: null,
};

describe("MarkTaskInstanceAsDialog", () => {
  it("does not select downstream by default", () => {
    render(<MarkTaskInstanceAsDialog onClose={vi.fn()} open state="success" taskInstance={taskInstance} />, {
      wrapper: Wrapper,
    });

    expect(screen.getByRole("button", { name: /downstream/iu })).not.toHaveAttribute("data-selected");
  });
});
