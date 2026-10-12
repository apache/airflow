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

import "@testing-library/jest-dom/vitest";
import { render, screen } from "@testing-library/react";
import { MemoryRouter, Route, Routes } from "react-router-dom";
import { beforeEach, describe, expect, it, vi } from "vitest";

import type { ReactAppResponse } from "openapi/requests/types.gen";

import { BaseWrapper, Wrapper } from "src/utils/Wrapper";

import { Overview } from "./Overview";

const latestTaskInstance = {
  dag_id: "my_dag",
  dag_run_id: "manual__run_1",
  end_date: "2026-10-09T08:02:00Z",
  logical_date: null,
  map_index: -1,
  run_after: "2026-10-09T08:00:00Z",
  start_date: "2026-10-09T08:00:00Z",
  state: "success",
  task_id: "my_task",
};

const withLatestTaskInstance = (overrides: Partial<typeof latestTaskInstance> = {}) => ({
  latest: {
    data: { task_instances: [{ ...latestTaskInstance, ...overrides }], total_entries: 1 },
    isLoading: false,
  },
});

type TaskInstancesResult = {
  data: { task_instances: Array<typeof latestTaskInstance>; total_entries: number };
  isLoading: boolean;
};

const emptyResult: TaskInstancesResult = {
  data: { task_instances: [], total_entries: 0 },
  isLoading: false,
};

const { mockUseGridServiceGetDagStructure, mockUseTaskInstanceServiceGetTaskInstances } = vi.hoisted(() => ({
  mockUseGridServiceGetDagStructure: vi.fn((): { data: Array<unknown> | undefined } => ({ data: undefined })),
  mockUseTaskInstanceServiceGetTaskInstances: vi.fn((..._args: Array<unknown>): TaskInstancesResult => ({
    data: { task_instances: [], total_entries: 0 },
    isLoading: false,
  })),
}));

/** The page calls the same hook twice; only the failed-instance call passes a state filter. */
const mockTaskInstances = ({
  failed = emptyResult,
  latest = emptyResult,
}: {
  failed?: TaskInstancesResult;
  latest?: TaskInstancesResult;
}) => {
  mockUseTaskInstanceServiceGetTaskInstances.mockImplementation((params) =>
    (params as { state?: Array<string> }).state === undefined ? latest : failed,
  );
};

const wrapperWithSearch = (search: string) => {
  const RouterWrapper = ({ children }: PropsWithChildren) => (
    <BaseWrapper>
      <MemoryRouter initialEntries={[`/dags/my_dag/tasks/my_task${search}`]}>{children}</MemoryRouter>
    </BaseWrapper>
  );

  return RouterWrapper;
};

vi.mock("openapi/queries", () => ({
  useGridServiceGetDagStructure: mockUseGridServiceGetDagStructure,
  useGridServiceGetLoopHistory: () => ({ data: undefined }),
  usePluginServiceGetPlugins: () => ({
    data: {
      plugins: [
        {
          react_apps: [
            { bundle_url: "/dag.js", destination: "dag_overview", name: "Dag overview plugin" },
            { bundle_url: "/task.js", destination: "task_overview", name: "Task overview plugin" },
            {
              applies_to: { "class_ref.class_name": ["PythonOperator"] },
              bundle_url: "/scoped.js",
              destination: "task_overview",
              name: "Scoped overview plugin",
            },
          ],
        },
      ],
    },
  }),
  useTaskInstanceServiceGetTaskInstances: mockUseTaskInstanceServiceGetTaskInstances,
}));

vi.mock("./LoopHistoryChart", () => ({ LoopHistoryChart: () => <div>loop history chart</div> }));
vi.mock("src/components/DurationChart", () => ({ DurationChart: () => null }));
vi.mock("src/components/NeedsReviewButton", () => ({ NeedsReviewButton: () => null }));
vi.mock("src/components/TimeRangeSelector", () => ({ default: () => null }));
vi.mock("src/components/TrendCountButton", () => ({
  TrendCountButton: ({ label }: { readonly label: string }) => (
    <div data-testid="trend-count-button">{label}</div>
  ),
}));
vi.mock("src/hooks/usePluginAppliesToContext", () => ({
  usePluginAppliesToContext: () => ({
    isLoading: false,
    task: { class_ref: { class_name: "BashOperator" }, operator_name: "BashOperator", task_id: "run_it" },
  }),
}));
vi.mock("src/pages/ReactPlugin", () => ({
  ReactPlugin: ({ reactApp }: { readonly reactApp: ReactAppResponse }) => <div>{reactApp.name}</div>,
}));
vi.mock("src/utils", () => ({
  isStatePending: () => false,
  useAutoRefresh: () => false,
}));
// ``src/utils/links`` reaches the route table in ``src/router``, which pulls the whole page tree —
// and this file's ``openapi/queries`` mock — back in. Stub the one export it needs from there.
vi.mock("src/router", () => ({ taskInstanceRoutes: [] }));

describe("Task overview plugins", () => {
  it("renders only React plugins registered for the task overview", () => {
    render(<Overview />, { wrapper: Wrapper });

    expect(screen.getByText("Task overview plugin")).toBeInTheDocument();
    expect(screen.queryByText("Dag overview plugin")).not.toBeInTheDocument();
  });

  it("omits a plugin whose applies_to does not match the task in scope", () => {
    render(<Overview />, { wrapper: Wrapper });

    expect(screen.queryByText("Scoped overview plugin")).not.toBeInTheDocument();
  });
});

describe("Task overview duration chart limit", () => {
  beforeEach(() => {
    mockUseTaskInstanceServiceGetTaskInstances.mockClear();
  });

  it("requests the default number of task instances when no limit is set", () => {
    render(<Overview />, { wrapper: wrapperWithSearch("") });

    expect(mockUseTaskInstanceServiceGetTaskInstances).toHaveBeenCalledWith(
      expect.objectContaining({ limit: 10, orderBy: ["-run_after"] }),
      undefined,
      expect.anything(),
    );
  });

  it("requests the number of task instances given by the limit search param", () => {
    render(<Overview />, { wrapper: wrapperWithSearch("?limit=50") });

    expect(mockUseTaskInstanceServiceGetTaskInstances).toHaveBeenCalledWith(
      expect.objectContaining({ limit: 50, orderBy: ["-run_after"] }),
      undefined,
      expect.anything(),
    );
  });
});

const renderGroupOverview = () =>
  render(
    <BaseWrapper>
      <MemoryRouter initialEntries={["/dags/my_dag/tasks/group/body"]}>
        <Routes>
          <Route element={<Overview />} path="/dags/:dagId/tasks/group/:groupId" />
        </Routes>
      </MemoryRouter>
    </BaseWrapper>,
  );

describe("Task overview loop history", () => {
  beforeEach(() => {
    mockUseGridServiceGetDagStructure.mockReset();
  });

  it("renders the loop history for a loop group", () => {
    mockUseGridServiceGetDagStructure.mockReturnValue({
      data: [{ children: [], id: "body", is_loop: true, is_mapped: false, label: "body" }],
    });
    renderGroupOverview();

    expect(screen.getByText("loop history chart")).toBeInTheDocument();
  });

  it("omits the loop history for a group that is not a loop", () => {
    mockUseGridServiceGetDagStructure.mockReturnValue({
      data: [{ children: [], id: "body", is_loop: false, is_mapped: false, label: "body" }],
    });
    renderGroupOverview();

    expect(screen.queryByText("loop history chart")).not.toBeInTheDocument();
  });
});

describe("Task overview failed task instance button", () => {
  it("is hidden when nothing failed in the time range", () => {
    mockTaskInstances({});

    render(<Overview />, { wrapper: Wrapper });

    expect(screen.queryByTestId("trend-count-button")).not.toBeInTheDocument();
  });

  it("is shown once task instances have failed", () => {
    mockTaskInstances({
      failed: { data: { task_instances: [latestTaskInstance], total_entries: 1 }, isLoading: false },
    });

    render(<Overview />, { wrapper: Wrapper });

    expect(screen.getByTestId("trend-count-button")).toHaveTextContent("overview.buttons.failedTaskInstance");
  });
});

describe("Task overview latest run card", () => {
  it("is omitted when the task has never run", () => {
    mockTaskInstances({});

    render(<Overview />, { wrapper: Wrapper });

    expect(screen.queryByTestId("latest-run-card")).not.toBeInTheDocument();
  });

  it("links to the latest task instance", () => {
    mockTaskInstances(withLatestTaskInstance());

    render(<Overview />, { wrapper: Wrapper });

    const card = screen.getByTestId("latest-run-card");

    expect(card).toHaveTextContent("manual__run_1");
    expect(card).toHaveAttribute("href", "/dags/my_dag/runs/manual__run_1/tasks/my_task");
  });

  it("points a mapped instance at its own map index", () => {
    mockTaskInstances(withLatestTaskInstance({ map_index: 2 }));

    render(<Overview />, { wrapper: Wrapper });

    expect(screen.getByTestId("latest-run-card")).toHaveAttribute(
      "href",
      "/dags/my_dag/runs/manual__run_1/tasks/my_task/mapped/2",
    );
  });
});
