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
import type { PropsWithChildren } from "react";

import { ChakraProvider, defaultSystem } from "@chakra-ui/react";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import "@testing-library/jest-dom/vitest";
import { fireEvent, render, screen } from "@testing-library/react";
import { Link, MemoryRouter, Route, Routes, useLocation } from "react-router-dom";
import { afterEach, describe, expect, it, vi } from "vitest";

import { UseTaskInstanceServiceGetMappedTaskInstanceKeyFn } from "openapi/queries";
import { TaskInstanceService, type TaskInstanceResponse } from "openapi/requests";

import { NavTabs, type NavTab } from "src/layouts/Details/NavTabs";

import { TaskInstance } from "./TaskInstance";

vi.mock("src/router", () => ({ taskInstanceRoutes: [] }));

vi.mock("src/hooks/useHITLReviewTabs", () => ({
  useHITLReviewTabs: vi.fn((_params: unknown, tabs: Array<NavTab>) => ({ tabs })),
}));
vi.mock("src/hooks/usePluginTabs", () => ({
  usePluginTabs: vi.fn(() => []),
}));
vi.mock("src/hooks/useRequiredActionTabs", () => ({
  useRequiredActionTabs: vi.fn((_params: unknown, tabs: Array<NavTab>) => ({ tabs })),
}));
vi.mock("src/layouts/Details/DetailsLayout", () => ({
  DetailsLayout: ({ children, tabs }: PropsWithChildren<{ readonly tabs: Array<NavTab> }>) => (
    <>
      {children}
      <NavTabs tabs={tabs} />
    </>
  ),
}));
vi.mock("src/queries/useGridTISummaries.ts", () => ({
  useGridTiSummariesStream: vi.fn(() => ({ summariesByRunId: new Map() })),
}));
vi.mock("src/utils", async () => {
  const actual = await vi.importActual("src/utils");

  return {
    ...actual,
    useAutoRefresh: vi.fn(() => false),
    useDocumentTitle: vi.fn(),
  };
});
vi.mock("./Header", () => ({
  Header: ({ taskInstance }: { readonly taskInstance: TaskInstanceResponse }) => (
    <div data-testid="task-instance-state">
      {taskInstance.task_id}:{taskInstance.state ?? "none"}:{taskInstance.try_number}
    </div>
  ),
}));

const DAG_ID = "test_dag";
const DAG_RUN_ID = "test_run";
const TASK_A = "task_a";
const TASK_B = "task_b";

const buildTaskInstance = (
  taskId: string,
  state: TaskInstanceResponse["state"],
  tryNumber: number,
): TaskInstanceResponse =>
  ({
    dag_id: DAG_ID,
    dag_run_id: DAG_RUN_ID,
    id: `${taskId}-id`,
    map_index: -1,
    state,
    task_display_name: taskId,
    task_id: taskId,
    try_number: tryNumber,
  }) as TaskInstanceResponse;

const buildTaskInstanceKey = (taskId: string) =>
  UseTaskInstanceServiceGetMappedTaskInstanceKeyFn({
    dagId: DAG_ID,
    dagRunId: DAG_RUN_ID,
    mapIndex: -1,
    taskId,
  });

const createWrapper =
  (queryClient: QueryClient) =>
  ({ children }: PropsWithChildren) => (
    <ChakraProvider value={defaultSystem}>
      <QueryClientProvider client={queryClient}>{children}</QueryClientProvider>
    </ChakraProvider>
  );

afterEach(() => vi.restoreAllMocks());

const Location = () => {
  const location = useLocation();

  return (
    <div data-testid="location">
      {location.pathname}
      {location.search}
    </div>
  );
};

describe("TaskInstance", () => {
  it.each(["", "?try_number=1&log_level=error"])(
    "keeps the selected try across task tabs with %s",
    async (search) => {
      const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } });

      vi.spyOn(TaskInstanceService, "getMappedTaskInstance").mockResolvedValue(
        buildTaskInstance(TASK_A, "up_for_retry", 2),
      );
      const path = `/dags/${DAG_ID}/runs/${DAG_RUN_ID}/tasks/${TASK_A}`;

      render(
        <MemoryRouter initialEntries={[`${path}/logs${search}`]}>
          <Location />
          <Routes>
            <Route element={<TaskInstance />} path="/dags/:dagId/runs/:runId/tasks/:taskId">
              <Route element={<div />} path="*" />
            </Route>
          </Routes>
        </MemoryRouter>,
        { wrapper: createWrapper(queryClient) },
      );
      expect(await screen.findByText(`${TASK_A}:up_for_retry:2`)).toBeTruthy();
      const expectedSearch = search ? "?try_number=1" : "";

      fireEvent.click(screen.getByRole("link", { name: "tabs.details" }));
      expect(screen.getByTestId("location")).toHaveTextContent(`${path}/details${expectedSearch}`);
      fireEvent.click(screen.getByRole("link", { name: "tabs.requiredActions" }));
      expect(screen.getByTestId("location")).toHaveTextContent(`${path}/required_actions${expectedSearch}`);
      fireEvent.click(screen.getByRole("link", { name: "tabs.logs" }));
      expect(screen.getByTestId("location")).toHaveTextContent(`${path}${expectedSearch}`);
      expect(screen.getByTestId("location")).not.toHaveTextContent("log_level");
    },
  );

  it.each([
    ["tabs.auditLog", "events"],
    ["tabs.mappedTaskInstances_other", "task_instances"],
    ["tabs.renderedTemplates", "rendered_templates"],
    ["tabs.storage", "xcom"],
    ["tabs.assetEvents", "asset_events"],
    ["tabs.code", "code"],
  ])("does not carry try selection into %s", async (label, destination) => {
    const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } });

    vi.spyOn(TaskInstanceService, "getMappedTaskInstance").mockResolvedValue({
      ...buildTaskInstance(TASK_A, "success", 3),
      map_index: 1,
    });
    const path = `/dags/${DAG_ID}/runs/${DAG_RUN_ID}/tasks/${TASK_A}/mapped/1`;

    render(
      <MemoryRouter initialEntries={[`${path}/logs?try_number=2`]}>
        <Location />
        <Routes>
          <Route element={<TaskInstance />} path="/dags/:dagId/runs/:runId/tasks/:taskId/mapped/:mapIndex">
            <Route element={<div />} path="*" />
          </Route>
        </Routes>
      </MemoryRouter>,
      { wrapper: createWrapper(queryClient) },
    );
    expect(await screen.findByText(`${TASK_A}:success:3`)).toBeTruthy();

    fireEvent.click(screen.getByRole("link", { name: label }));

    expect(screen.getByTestId("location").textContent).toBe(`${path}/${destination}`);
  });

  it("refetches a cached task instance immediately when switching tasks", async () => {
    const queryClient = new QueryClient({
      defaultOptions: {
        queries: {
          retry: false,
          staleTime: 5 * 60 * 1000,
        },
      },
    });
    const cachedTaskA = buildTaskInstance(TASK_A, null, 2);
    const latestTaskA = buildTaskInstance(TASK_A, "success", 3);
    const taskB = buildTaskInstance(TASK_B, "success", 1);

    queryClient.setQueryData(buildTaskInstanceKey(TASK_A), cachedTaskA);
    queryClient.setQueryData(buildTaskInstanceKey(TASK_B), taskB);
    vi.spyOn(TaskInstanceService, "getMappedTaskInstance").mockImplementation(
      ({ taskId }) =>
        Promise.resolve(taskId === TASK_A ? latestTaskA : taskB) as unknown as ReturnType<
          typeof TaskInstanceService.getMappedTaskInstance
        >,
    );

    render(
      <MemoryRouter initialEntries={[`/dags/${DAG_ID}/runs/${DAG_RUN_ID}/tasks/${TASK_B}`]}>
        <Link to={`/dags/${DAG_ID}/runs/${DAG_RUN_ID}/tasks/${TASK_A}`}>Open task A</Link>
        <Routes>
          <Route element={<TaskInstance />} path="/dags/:dagId/runs/:runId/tasks/:taskId" />
        </Routes>
      </MemoryRouter>,
      { wrapper: createWrapper(queryClient) },
    );

    expect(await screen.findByText(`${TASK_B}:success:1`)).toBeTruthy();

    fireEvent.click(screen.getByRole("link", { name: "Open task A" }));

    expect(await screen.findByText(`${TASK_A}:success:3`)).toBeTruthy();
    expect(TaskInstanceService.getMappedTaskInstance).toHaveBeenCalledWith({
      dagId: DAG_ID,
      dagRunId: DAG_RUN_ID,
      mapIndex: -1,
      taskId: TASK_A,
    });
  });
});
