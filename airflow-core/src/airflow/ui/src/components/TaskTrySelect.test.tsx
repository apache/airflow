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
import { ChakraProvider, defaultSystem } from "@chakra-ui/react";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { render, screen } from "@testing-library/react";
import type { PropsWithChildren } from "react";
import { MemoryRouter } from "react-router-dom";
import { afterEach, describe, expect, it, vi } from "vitest";

import { UseTaskInstanceServiceGetMappedTaskInstanceTriesKeyFn } from "openapi/queries";
import {
  TaskInstanceService,
  type TaskInstanceHistoryCollectionResponse,
  type TaskInstanceHistoryResponse,
  type TaskInstanceResponse,
} from "openapi/requests";

import { TaskTrySelect } from "./TaskTrySelect";

vi.mock("src/utils", async () => {
  const actual = await vi.importActual("src/utils");

  return {
    ...actual,
    useAutoRefresh: vi.fn(() => false),
  };
});

const DAG_ID = "test_dag";
const DAG_RUN_ID = "test_run";
const TASK_A = "task_a";
const TASK_B = "task_b";

const buildTaskInstance = (taskId: string, tryNumber: number): TaskInstanceResponse =>
  ({
    dag_id: DAG_ID,
    dag_run_id: DAG_RUN_ID,
    id: `${taskId}-id`,
    map_index: -1,
    state: "success",
    task_display_name: taskId,
    task_id: taskId,
    try_number: tryNumber,
  }) as TaskInstanceResponse;

const buildTaskTry = (tryNumber: number): TaskInstanceHistoryResponse =>
  ({
    dag_id: DAG_ID,
    dag_run_id: DAG_RUN_ID,
    map_index: -1,
    state: "success",
    task_display_name: TASK_A,
    task_id: TASK_A,
    try_number: tryNumber,
  }) as TaskInstanceHistoryResponse;

const buildTaskTries = (tryNumbers: Array<number>): TaskInstanceHistoryCollectionResponse => ({
  task_instances: tryNumbers.map(buildTaskTry),
  total_entries: tryNumbers.length,
});

const createWrapper =
  (queryClient: QueryClient) =>
  ({ children }: PropsWithChildren) => (
    <ChakraProvider value={defaultSystem}>
      <QueryClientProvider client={queryClient}>
        <MemoryRouter>{children}</MemoryRouter>
      </QueryClientProvider>
    </ChakraProvider>
  );

afterEach(() => vi.restoreAllMocks());

describe("TaskTrySelect", () => {
  it("refetches cached tries immediately when switching tasks", async () => {
    const queryClient = new QueryClient({
      defaultOptions: {
        queries: {
          retry: false,
          staleTime: 5 * 60 * 1000,
        },
      },
    });
    const params = {
      dagId: DAG_ID,
      dagRunId: DAG_RUN_ID,
      mapIndex: -1,
      taskId: TASK_A,
    };

    queryClient.setQueryData(
      UseTaskInstanceServiceGetMappedTaskInstanceTriesKeyFn(params),
      buildTaskTries([1, 2]),
    );
    vi.spyOn(TaskInstanceService, "getMappedTaskInstanceTries").mockResolvedValue(buildTaskTries([1, 2, 3]));

    const { rerender } = render(
      <TaskTrySelect selectedTryNumber={1} taskInstance={buildTaskInstance(TASK_B, 1)} />,
      { wrapper: createWrapper(queryClient) },
    );

    rerender(<TaskTrySelect selectedTryNumber={3} taskInstance={buildTaskInstance(TASK_A, 3)} />);

    expect(await screen.findByTestId("log-attempt-select-button-3")).toBeTruthy();
    expect(
      screen
        .getAllByTestId(/^log-attempt-select-button-/u)
        .map((button) => button.getAttribute("data-testid")),
    ).toEqual(["log-attempt-select-button-1", "log-attempt-select-button-2", "log-attempt-select-button-3"]);
    expect(TaskInstanceService.getMappedTaskInstanceTries).toHaveBeenCalledWith(params);
  });
});
