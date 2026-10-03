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
import { fireEvent, render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";

import type { TaskInstanceResponse } from "openapi/requests/types.gen";

import { usePatchTaskInstanceDryRun } from "src/queries/usePatchTaskInstanceDryRun";
import { Wrapper } from "src/utils/Wrapper";

import MarkTaskInstanceAsDialog from "./MarkTaskInstanceAsDialog";

const { mutate } = vi.hoisted(() => ({ mutate: vi.fn() }));

vi.mock("src/queries/usePatchTaskInstance", () => ({
  usePatchTaskInstance: () => ({
    isPending: false,
    mutate,
  }),
}));

vi.mock("src/queries/usePatchTaskInstanceDryRun", () => ({
  usePatchTaskInstanceDryRun: vi.fn(() => ({
    data: {
      task_instances: [],
      total_entries: 0,
    },
    isPending: false,
  })),
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
  region_id: "00000000-0000-0000-0000-000000000000",
  region_index: 0,
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
  it("marks an exact regional execution and disables cross-run scope", () => {
    const regional = {
      ...taskInstance,
      map_index: -1,
      region_id: "11111111-1111-4111-8111-111111111111",
      region_index: 3,
    };

    render(<MarkTaskInstanceAsDialog onClose={vi.fn()} open state="success" taskInstance={regional} />, {
      wrapper: Wrapper,
    });
    expect(screen.getByRole("button", { name: /past/iu })).toBeDisabled();
    expect(screen.getByRole("button", { name: /future/iu })).toBeDisabled();
    expect(vi.mocked(usePatchTaskInstanceDryRun).mock.lastCall?.[0]).toMatchObject({
      requestBody: {
        include_future: false,
        include_past: false,
        region_id: regional.region_id,
        region_index: 3,
      },
    });
    fireEvent.click(screen.getByRole("button", { name: /confirm/iu }));
    expect(mutate.mock.lastCall?.[0]).toMatchObject({
      requestBody: {
        include_future: false,
        include_past: false,
        region_id: regional.region_id,
        region_index: 3,
      },
    });
  });
  it("does not select downstream by default", () => {
    render(<MarkTaskInstanceAsDialog onClose={vi.fn()} open state="success" taskInstance={taskInstance} />, {
      wrapper: Wrapper,
    });

    expect(screen.getByRole("button", { name: /downstream/iu })).not.toHaveAttribute("data-selected");
  });
});
