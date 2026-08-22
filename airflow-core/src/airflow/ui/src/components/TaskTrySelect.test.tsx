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
import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import type { PropsWithChildren, ReactNode } from "react";
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

vi.mock("src/components/StateBadge", () => ({
  StateBadge: ({ children, state }: { readonly children?: ReactNode; readonly state?: string | null }) => (
    <span data-state={state}>{children}</span>
  ),
}));

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

const buildTaskInstance = (
  taskId: string,
  tryNumber: number,
  {
    mapIndex = -1,
    state = "success",
  }: {
    readonly mapIndex?: number;
    readonly state?: TaskInstanceResponse["state"];
  } = {},
): TaskInstanceResponse =>
  ({
    dag_id: DAG_ID,
    dag_run_id: DAG_RUN_ID,
    id: `${taskId}-${mapIndex}`,
    map_index: mapIndex,
    state,
    task_display_name: taskId,
    task_id: taskId,
    try_number: tryNumber,
  }) as TaskInstanceResponse;

const buildTaskTry = (
  taskId: string,
  tryNumber: number,
  {
    mapIndex = -1,
    state = "success",
  }: {
    readonly mapIndex?: number;
    readonly state?: TaskInstanceHistoryResponse["state"];
  } = {},
): TaskInstanceHistoryResponse => ({
  ...buildTaskInstance(taskId, tryNumber, { mapIndex, state }),
});

const buildTaskTries = (
  taskInstances: Array<TaskInstanceHistoryResponse>,
): TaskInstanceHistoryCollectionResponse => ({
  task_instances: taskInstances,
  total_entries: taskInstances.length,
});

const createQueryClient = () =>
  new QueryClient({
    defaultOptions: {
      queries: {
        retry: false,
        staleTime: 5 * 60 * 1000,
      },
    },
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

const expectTries = async (tries: Array<number>) => {
  await waitFor(() => {
    expect(
      screen
        .getAllByTestId(/^log-attempt-select-button-/u)
        .map((button) => button.getAttribute("data-testid")),
    ).toEqual(tries.map((tryNumber) => `log-attempt-select-button-${tryNumber}`));
  });
  expect(screen.queryByTestId("log-attempt-select-button-0")).toBeNull();
};

const expectTryState = (tryNumber: number, state: string) => {
  expect(
    screen
      .getByTestId(`log-attempt-select-button-${tryNumber}`)
      .querySelector("[data-state]")
      ?.getAttribute("data-state"),
  ).toBe(state);
};

afterEach(() => vi.restoreAllMocks());

describe("TaskTrySelect", () => {
  it("refetches cached tries immediately when switching tasks", async () => {
    const queryClient = createQueryClient();
    const params = {
      dagId: DAG_ID,
      dagRunId: DAG_RUN_ID,
      mapIndex: -1,
      taskId: TASK_A,
    };

    queryClient.setQueryData(
      UseTaskInstanceServiceGetMappedTaskInstanceTriesKeyFn(params),
      buildTaskTries([1, 2].map((tryNumber) => buildTaskTry(TASK_A, tryNumber))),
    );
    vi.spyOn(TaskInstanceService, "getMappedTaskInstanceTries").mockResolvedValue(
      buildTaskTries([1, 2, 3].map((tryNumber) => buildTaskTry(TASK_A, tryNumber))),
    );

    const { rerender } = render(
      <TaskTrySelect selectedTryNumber={1} taskInstance={buildTaskInstance(TASK_B, 1)} />,
      { wrapper: createWrapper(queryClient) },
    );

    rerender(<TaskTrySelect selectedTryNumber={3} taskInstance={buildTaskInstance(TASK_A, 3)} />);

    await expectTries([1, 2, 3]);
    expect(TaskInstanceService.getMappedTaskInstanceTries).toHaveBeenCalledWith(params);
  });

  it("keeps positive tries unique while switching between task instances", async () => {
    const queryClient = createQueryClient();
    const histories = {
      start: buildTaskTries([1, 2, 2, 3, 4].map((tryNumber) => buildTaskTry("start", tryNumber))),
      task_1: buildTaskTries([
        buildTaskTry("task_1", 0, { state: "skipped" }),
        buildTaskTry("task_1", 1),
        buildTaskTry("task_1", 2),
      ]),
      task_2: buildTaskTries([buildTaskTry("task_2", 1), buildTaskTry("task_2", 2)]),
    };

    const getTries = vi
      .spyOn(TaskInstanceService, "getMappedTaskInstanceTries")
      .mockResolvedValue(histories.start);

    const { rerender } = render(<TaskTrySelect taskInstance={buildTaskInstance("start", 4)} />, {
      wrapper: createWrapper(queryClient),
    });

    await expectTries([1, 2, 3, 4]);

    getTries.mockResolvedValue(histories.task_1);
    rerender(<TaskTrySelect taskInstance={buildTaskInstance("task_1", 2)} />);
    await expectTries([1, 2]);

    getTries.mockResolvedValue(histories.task_2);
    rerender(<TaskTrySelect taskInstance={buildTaskInstance("task_2", 2)} />);
    await expectTries([1, 2]);

    getTries.mockResolvedValue(histories.task_1);
    rerender(<TaskTrySelect taskInstance={buildTaskInstance("task_1", 2)} />);
    await expectTries([1, 2]);

    getTries.mockResolvedValue(histories.start);
    rerender(<TaskTrySelect taskInstance={buildTaskInstance("start", 4)} />);
    await expectTries([1, 2, 3, 4]);
  });

  it("uses a real current try but not retry or null placeholders", async () => {
    const queryClient = createQueryClient();
    const params = {
      dagId: DAG_ID,
      dagRunId: DAG_RUN_ID,
      mapIndex: 1,
      taskId: "mapped_task",
    };
    const history = buildTaskTries([
      buildTaskTry("mapped_task", 1, { mapIndex: 1 }),
      buildTaskTry("mapped_task", 2, { mapIndex: 1, state: "failed" }),
    ]);
    const onSelectTryNumber = vi.fn();

    queryClient.setQueryData(UseTaskInstanceServiceGetMappedTaskInstanceTriesKeyFn(params), history);
    vi.spyOn(TaskInstanceService, "getMappedTaskInstanceTries").mockResolvedValue(history);

    const { rerender } = render(
      <TaskTrySelect
        onSelectTryNumber={onSelectTryNumber}
        selectedTryNumber={2}
        taskInstance={buildTaskInstance("mapped_task", 2, { mapIndex: 1 })}
      />,
      { wrapper: createWrapper(queryClient) },
    );

    await expectTries([1, 2]);
    expectTryState(2, "success");

    rerender(
      <TaskTrySelect
        onSelectTryNumber={onSelectTryNumber}
        selectedTryNumber={2}
        taskInstance={buildTaskInstance("mapped_task", 2, { mapIndex: 1, state: "up_for_retry" })}
      />,
    );
    expectTryState(2, "failed");

    rerender(
      <TaskTrySelect
        onSelectTryNumber={onSelectTryNumber}
        selectedTryNumber={2}
        taskInstance={buildTaskInstance("mapped_task", 2, { mapIndex: 1, state: null })}
      />,
    );
    expectTryState(2, "failed");

    fireEvent.click(screen.getByTestId("log-attempt-select-button-1"));
    expect(onSelectTryNumber).toHaveBeenCalledOnce();
    expect(onSelectTryNumber).toHaveBeenCalledWith(1);
    expect(TaskInstanceService.getMappedTaskInstanceTries).toHaveBeenCalledWith(params);

    rerender(<TaskTrySelect taskInstance={buildTaskInstance("mapped_task", 0, { mapIndex: 1 })} />);
    expect(screen.queryByTestId(/^log-attempt-select-button-/u)).toBeNull();
  });
});
