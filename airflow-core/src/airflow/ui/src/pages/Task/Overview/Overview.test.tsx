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
import type { PropsWithChildren } from "react";
import { MemoryRouter } from "react-router-dom";
import { beforeEach, describe, expect, it, vi } from "vitest";

import type { ReactAppResponse } from "openapi/requests/types.gen";
import { BaseWrapper, Wrapper } from "src/utils/Wrapper";

import { Overview } from "./Overview";

const { mockUseTaskInstanceServiceGetTaskInstances } = vi.hoisted(() => ({
  mockUseTaskInstanceServiceGetTaskInstances: vi.fn(() => ({
    data: { task_instances: [], total_entries: 0 },
    isLoading: false,
  })),
}));

const wrapperWithSearch = (search: string) => {
  const RouterWrapper = ({ children }: PropsWithChildren) => (
    <BaseWrapper>
      <MemoryRouter initialEntries={[`/dags/my_dag/tasks/my_task${search}`]}>{children}</MemoryRouter>
    </BaseWrapper>
  );

  return RouterWrapper;
};

vi.mock("openapi/queries", () => ({
  usePluginServiceGetPlugins: () => ({
    data: {
      plugins: [
        {
          react_apps: [
            { bundle_url: "/dag.js", destination: "dag_overview", name: "Dag overview plugin" },
            { bundle_url: "/task.js", destination: "task_overview", name: "Task overview plugin" },
            {
              applies_to: { operators: ["PythonOperator"] },
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

vi.mock("src/components/DurationChart", () => ({ DurationChart: () => null }));
vi.mock("src/components/NeedsReviewButton", () => ({ NeedsReviewButton: () => null }));
vi.mock("src/components/TimeRangeSelector", () => ({ default: () => null }));
vi.mock("src/components/TrendCountButton", () => ({ TrendCountButton: () => null }));
vi.mock("src/hooks/usePluginAppliesToContext", () => ({
  usePluginAppliesToContext: () => ({
    isLoading: false,
    task: { class_ref: { class_name: "BashOperator" }, operator_name: "BashOperator", task_id: "run_it" },
  }),
}));
vi.mock("src/pages/ReactPlugin", () => ({
  ReactPlugin: ({ reactApp }: { readonly reactApp: ReactAppResponse }) => <div>{reactApp.name}</div>,
}));
vi.mock("src/utils", () => ({ isStatePending: () => false, useAutoRefresh: () => false }));

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
