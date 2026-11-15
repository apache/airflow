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
import type { ReactNode } from "react";

import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import "@testing-library/jest-dom/vitest";
import { fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import { http, HttpResponse } from "msw";
import { afterEach, expect, it, vi } from "vitest";

import type { ClearTaskInstancesBody, TaskInstanceResponse } from "openapi/requests/types.gen";

import {
  CLEAR_PREVENT_RUNNING_TASK_KEY,
  CLEAR_TASK_INSTANCE_DEFAULT_OPTIONS_KEY,
} from "src/constants/localStorage";
import { server } from "src/mocks/server";
import { useClearTaskInstancesDryRunKey } from "src/queries/useClearTaskInstancesDryRun";
import { Wrapper } from "src/utils/Wrapper";

import ClearTaskInstanceConfirmationDialog from "./ClearTaskInstanceConfirmationDialog";
import ClearTaskInstanceDialog from "./ClearTaskInstanceDialog";

const DAG_ID = "clear_error_test";
const DAG_RUN_ID = "run_1";
const ENDPOINT = `/api/v2/dags/${DAG_ID}/clearTaskInstances`;
const mutateMock = vi.fn();

vi.mock("src/queries/useClearTaskInstances", () => ({
  useClearTaskInstances: () => ({ isPending: false, mutate: mutateMock }),
}));

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
  id: "task-a",
  in_loop: false,
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
  task_display_name: "A",
  task_id: "A",
  trigger: null,
  triggerer_job: null,
  try_number: 1,
  unixname: null,
};
const affectedTasks = {
  task_instances: [
    taskInstance,
    { ...taskInstance, id: "task-b", task_display_name: "B", task_id: "B" },
    { ...taskInstance, id: "task-c", task_display_name: "C", task_id: "C" },
  ],
  total_entries: 3,
};

const createWrapper = (queryClient: QueryClient) => {
  const TestWrapper = ({ children }: { readonly children: ReactNode }) => (
    <Wrapper>
      <QueryClientProvider client={queryClient}>{children}</QueryClientProvider>
    </Wrapper>
  );

  return TestWrapper;
};

const createQueryClient = () => new QueryClient({ defaultOptions: { queries: { retry: false } } });

afterEach(() => {
  localStorage.clear();
  mutateMock.mockClear();
});

it.each(["network", "server"] as const)(
  "shows a dismissible error when the confirmation preview has a %s failure after excluding a task",
  async (failure) => {
    localStorage.setItem(CLEAR_PREVENT_RUNNING_TASK_KEY, JSON.stringify(true));
    localStorage.setItem(CLEAR_TASK_INSTANCE_DEFAULT_OPTIONS_KEY, JSON.stringify(["downstream"]));
    const confirmationRequests: Array<ClearTaskInstancesBody> = [];

    server.use(
      http.get(`/api/v2/dags/${DAG_ID}/details`, () => HttpResponse.json({})),
      http.get(`/api/v2/dags/${DAG_ID}/dagRuns/${DAG_RUN_ID}`, () => HttpResponse.json({ dag_versions: [] })),
      http.post(ENDPOINT, async ({ request }) => {
        const body = (await request.json()) as ClearTaskInstancesBody;

        if (body.task_instance_ids !== undefined) {
          confirmationRequests.push(body);

          return failure === "network"
            ? HttpResponse.error()
            : HttpResponse.json({ detail: "Preview unavailable" }, { status: 500 });
        }

        return HttpResponse.json(affectedTasks);
      }),
    );
    render(<ClearTaskInstanceDialog onClose={vi.fn()} open taskInstance={taskInstance} />, {
      wrapper: createWrapper(createQueryClient()),
    });

    const excludedRow = (await screen.findByText("B")).closest("tr");

    expect(excludedRow).not.toBeNull();
    fireEvent.click(within(excludedRow as HTMLElement).getByRole("checkbox"));
    fireEvent.click(screen.getByRole("button", { name: /modal\.confirm/iu }));

    const error = await screen.findByTestId("error-alert");

    expect(error).toHaveTextContent(failure === "network" ? /network error/iu : "Preview unavailable");
    expect(confirmationRequests).toEqual([
      expect.objectContaining({ dry_run: true, task_instance_ids: ["task-a", "task-c"] }),
    ]);
    expect(mutateMock).not.toHaveBeenCalled();
    fireEvent.click(screen.getByRole("button", { name: /modal\.cancel/iu }));
    await waitFor(() => expect(screen.queryByTestId("error-alert")).not.toBeInTheDocument());
    expect(mutateMock).not.toHaveBeenCalled();
  },
);

it("does not clear using cached preview data when its confirmation refetch fails", async () => {
  const queryClient = createQueryClient();
  const requestBody = { dag_run_id: DAG_RUN_ID, task_ids: ["A"] };
  const onConfirm = vi.fn();
  const onClose = vi.fn();

  queryClient.setQueryData([useClearTaskInstancesDryRunKey, DAG_ID, requestBody], affectedTasks);
  server.use(
    http.post(ENDPOINT, () => HttpResponse.json({ detail: "Preview unavailable" }, { status: 500 })),
  );
  render(
    <ClearTaskInstanceConfirmationDialog
      dryRun={{ dagId: DAG_ID, requestBody }}
      onClose={onClose}
      onConfirm={onConfirm}
      open
      preventRunningTask
    />,
    { wrapper: createWrapper(queryClient) },
  );

  expect(await screen.findByTestId("error-alert")).toHaveTextContent("Preview unavailable");
  expect(onConfirm).not.toHaveBeenCalled();
  expect(onClose).not.toHaveBeenCalled();
  fireEvent.click(screen.getByRole("button", { name: /modal\.cancel/iu }));
  expect(onClose).toHaveBeenCalledOnce();
});
