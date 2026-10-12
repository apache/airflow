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
import { render, screen, within } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";

import type { ReactAppResponse } from "openapi/requests/types.gen";

import { Wrapper } from "src/utils/Wrapper";
import type * as StateUtils from "src/utils/stateUtils";

import { Overview } from "./Overview";

const latestRun = {
  dag_id: "my_dag",
  duration: 120,
  end_date: "2026-10-09T08:02:00Z",
  id: 7,
  logical_date: "2026-10-09T08:00:00Z",
  run_after: "2026-10-09T08:00:00Z",
  run_id: "manual__run_1",
  start_date: "2026-10-09T08:00:00Z",
  state: "success",
};

type LatestRunResult = { data: typeof latestRun | null | undefined; isLoading: boolean };
type StateCountsResult = {
  data: { dags: Array<{ dag_id: string; state_counts: Record<string, number> }> } | undefined;
  isLoading: boolean;
};
type DagRunsResult = { data: { dag_runs: Array<{ run_after: string }>; total_entries: number } };
type TaskInstancesResult = {
  data: {
    task_instances: Array<{ logical_date: string | null; start_date: string | null }>;
    total_entries: number;
  };
};

const { mockUseDagRunServiceGetDagRuns, mockUseLatestRunInfo, mockUseStateCounts, mockUseTaskInstances } =
  vi.hoisted(() => ({
    mockUseDagRunServiceGetDagRuns: vi.fn((): DagRunsResult => ({
      data: { dag_runs: [], total_entries: 0 },
    })),
    mockUseLatestRunInfo: vi.fn((): LatestRunResult => ({ data: undefined, isLoading: false })),
    mockUseStateCounts: vi.fn((): StateCountsResult => ({ data: undefined, isLoading: false })),
    mockUseTaskInstances: vi.fn((): TaskInstancesResult => ({
      data: { task_instances: [], total_entries: 0 },
    })),
  }));

vi.mock("openapi/queries", () => ({
  useAssetServiceGetAssetEvents: () => ({ data: undefined, isLoading: false }),
  useDagRunServiceGetDagRuns: mockUseDagRunServiceGetDagRuns,
  useDagServiceGetLatestRunInfo: mockUseLatestRunInfo,
  useDagServiceGetRecentTaskInstanceStateCountsUi: mockUseStateCounts,
  usePluginServiceGetPlugins: () => ({
    data: {
      plugins: [
        {
          react_apps: [
            { bundle_url: "/dag.js", destination: "dag_overview", name: "Dag overview plugin" },
            { bundle_url: "/task.js", destination: "task_overview", name: "Task overview plugin" },
            {
              applies_to: { "dag.tags.name": ["finance"] },
              bundle_url: "/scoped.js",
              destination: "dag_overview",
              name: "Scoped overview plugin",
            },
          ],
        },
      ],
    },
  }),
  useTaskInstanceServiceGetTaskInstances: mockUseTaskInstances,
}));

vi.mock("src/components/Assets/AssetEvents", () => ({ AssetEvents: () => null }));
vi.mock("src/components/DurationChart", () => ({ DurationChart: () => null }));
vi.mock("src/components/SlowestTaskInstancesChart", () => ({ SlowestTaskInstancesChart: () => null }));
vi.mock("src/components/TimeRangeSelector", () => ({ default: () => null }));
vi.mock("src/components/TrendCountButton", () => ({
  TrendCountButton: ({ label }: { readonly label: string }) => (
    <div data-testid="trend-count-button">{label}</div>
  ),
}));
vi.mock("src/hooks/usePluginAppliesToContext", () => ({
  usePluginAppliesToContext: () => ({
    dag: { dag_id: "etl_sales", tags: [{ dag_id: "etl_sales", name: "ml" }] },
    isLoading: false,
  }),
}));
vi.mock("src/pages/ReactPlugin", () => ({
  ReactPlugin: ({ reactApp }: { readonly reactApp: ReactAppResponse }) => <div>{reactApp.name}</div>,
}));
vi.mock("src/queries/useGridRuns.ts", () => ({ useGridRuns: () => ({ data: [], isLoading: false }) }));
// Keep the real state ordering so the badge-order assertion below is meaningful.
vi.mock("src/utils", async () => {
  const { sortStateEntries } = await vi.importActual<typeof StateUtils>("src/utils/stateUtils");

  return { isStatePending: () => false, sortStateEntries, useAutoRefresh: () => false };
});
vi.mock("./DagDeadlines", () => ({ DagDeadlines: () => null }));
vi.mock("./FailedLogs", () => ({ default: () => null }));

describe("Dag overview plugins", () => {
  it("renders only React plugins registered for the Dag overview", () => {
    render(<Overview />, { wrapper: Wrapper });

    expect(screen.getByText("Dag overview plugin")).toBeInTheDocument();
    expect(screen.queryByText("Task overview plugin")).not.toBeInTheDocument();
  });

  it("omits a plugin whose applies_to does not match the Dag in scope", () => {
    render(<Overview />, { wrapper: Wrapper });

    expect(screen.queryByText("Scoped overview plugin")).not.toBeInTheDocument();
  });
});

describe("Dag overview failure buttons", () => {
  it("hides both failure buttons when nothing failed in the time range", () => {
    render(<Overview />, { wrapper: Wrapper });

    expect(screen.queryAllByTestId("trend-count-button")).toHaveLength(0);
  });

  it("shows the failed task button once tasks have failed", () => {
    mockUseTaskInstances.mockReturnValue({
      data: {
        task_instances: [{ logical_date: null, start_date: latestRun.start_date }],
        total_entries: 1,
      },
    });

    render(<Overview />, { wrapper: Wrapper });

    expect(screen.getByText("overview.buttons.failedTask")).toBeInTheDocument();
    expect(screen.queryByText("overview.buttons.failedRun")).not.toBeInTheDocument();
  });

  it("shows the failed run button once runs have failed", () => {
    mockUseDagRunServiceGetDagRuns.mockReturnValue({
      data: { dag_runs: [{ run_after: latestRun.run_after }], total_entries: 1 },
    });

    render(<Overview />, { wrapper: Wrapper });

    expect(screen.getByText("overview.buttons.failedRun")).toBeInTheDocument();
    expect(screen.queryByText("overview.buttons.failedTask")).not.toBeInTheDocument();
  });
});

describe("Dag overview latest run card", () => {
  it("is omitted when the Dag has never run", () => {
    mockUseLatestRunInfo.mockReturnValue({ data: null, isLoading: false });

    render(<Overview />, { wrapper: Wrapper });

    expect(screen.queryByTestId("latest-run-card")).not.toBeInTheDocument();
  });

  it("links to the latest run", () => {
    mockUseLatestRunInfo.mockReturnValue({ data: latestRun, isLoading: false });

    render(<Overview />, { wrapper: Wrapper });

    const card = screen.getByTestId("latest-run-card");

    expect(card).toHaveTextContent("manual__run_1");
    expect(card).toHaveAttribute("href", "/dags/my_dag/runs/manual__run_1");
  });

  it("breaks the run down into task state badges, without nesting links in the card", () => {
    mockUseLatestRunInfo.mockReturnValue({ data: latestRun, isLoading: false });
    mockUseStateCounts.mockReturnValue({
      data: { dags: [{ dag_id: "my_dag", state_counts: { failed: 2, skipped: 0, success: 9 } }] },
      isLoading: false,
    });

    render(<Overview />, { wrapper: Wrapper });

    const card = screen.getByTestId("latest-run-card");
    const badges = within(card).getAllByTestId("state-badge");

    // The run's own state badge, then one badge per non-zero task state.
    expect(badges.map((badge) => badge.textContent)).toEqual(["states.success", "2", "9"]);
    expect(within(card).queryByRole("link")).not.toBeInTheDocument();
  });

  it("asks for the task state counts of the latest run only", () => {
    mockUseLatestRunInfo.mockReturnValue({ data: latestRun, isLoading: false });

    render(<Overview />, { wrapper: Wrapper });

    expect(mockUseStateCounts).toHaveBeenCalledWith(
      { dagRunIds: [7] },
      undefined,
      expect.objectContaining({ enabled: true }),
    );
  });
});
