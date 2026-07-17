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
import { act, fireEvent, render, screen } from "@testing-library/react";
import i18n from "i18next";
import type { DagTagResponse, DAGWithLatestDagRunsResponse } from "openapi-gen/requests/types.gen";
import type { PropsWithChildren } from "react";
import { MemoryRouter } from "react-router-dom";
import { afterEach, describe, it, vi, expect, beforeAll } from "vitest";

import { TimezoneProvider } from "src/context/timezone";
import { BaseWrapper } from "src/utils/Wrapper";

import "../../i18n/config";
import { DagCard } from "./DagCard";

// Mock the timezone context to always return UTC/GMT
vi.mock("src/context/timezone", async () => {
  const actual = await vi.importActual("src/context/timezone");

  return {
    ...actual,
    TimezoneProvider: ({ children }: PropsWithChildren) => children,
    useTimezone: () => ({
      selectedTimezone: "UTC",
      setSelectedTimezone: vi.fn(),
    }),
  };
});

// Custom wrapper that uses GMT timezone
const GMTWrapper = ({ children }: PropsWithChildren) => (
  <BaseWrapper>
    <MemoryRouter>
      <TimezoneProvider>{children}</TimezoneProvider>
    </MemoryRouter>
  </BaseWrapper>
);

// Render with the run-state-counts row in its loading state so it renders
// skeletons rather than StateBadges. Without this, every card would emit
// 4 extra "state-badge" testids and break getByTestId assertions in tests
// that target the latest-run badge.
const renderCard = (dag: DAGWithLatestDagRunsResponse) =>
  render(<DagCard dag={dag} runStateCounts={undefined} runStateCountsLoading stateCountLimit={undefined} />, {
    wrapper: GMTWrapper,
  });

const mockDag = {
  allowed_run_types: null,
  asset_expression: null,
  bundle_name: "dags-folder",
  bundle_version: "1",
  dag_display_name: "nested_groups",
  dag_id: "nested_groups",
  description: null,
  file_token: "Ii9maWxlcy9kYWdzL25lc3RlZF90YXNrX2dyb3Vwcy5weSI.G3EkdxmDUDQsVb7AIZww1TSGlFE",
  fileloc: "/files/dags/nested_task_groups.py",
  has_import_errors: false,
  has_task_concurrency_limits: false,
  is_backfillable: true,
  is_favorite: false,
  is_paused: false,
  is_stale: false,
  last_expired: null,
  last_parse_duration: 0.23,
  last_parsed_time: "2024-08-22T13:50:10.372238+00:00",
  latest_dag_runs: [
    {
      dag_id: "nested_groups",
      duration: 16.244,
      end_date: "2025-09-19T19:22:00.798715Z",
      id: 1,
      logical_date: "2025-09-19T19:22:00Z",
      run_after: "2025-09-19T19:22:00Z",
      run_id: "scheduled__2025-09-19T19:22:00+00:00",
      start_date: "2025-09-19T19:22:00.782471Z",
      state: "success",
    },
    {
      dag_id: "nested_groups",
      duration: 16.411,
      end_date: "2025-09-19T19:21:00.731218Z",
      id: 2,
      logical_date: "2025-09-19T19:21:00Z",
      run_after: "2025-09-19T19:21:00Z",
      run_id: "scheduled__2025-09-19T19:21:00+00:00",
      start_date: "2025-09-19T19:21:00.714807Z",
      state: "success",
    },
  ],
  max_active_runs: 16,
  max_active_tasks: 16,
  max_consecutive_failed_dag_runs: 0,
  next_dagrun_data_interval_end: "2024-08-23T00:00:00+00:00",
  next_dagrun_data_interval_start: "2024-08-22T00:00:00+00:00",
  next_dagrun_logical_date: "2024-08-22T00:00:00+00:00",
  next_dagrun_run_after: "2024-08-22T19:00:00+00:00",
  owners: ["airflow"],
  pending_actions: [],
  relative_fileloc: "nested_task_groups.py",
  tags: [],
  timetable_description: "Every minute",
  timetable_partitioned: false,
  timetable_periodic: true,
  timetable_summary: "* * * * *",
} satisfies DAGWithLatestDagRunsResponse;

beforeAll(async () => {
  await i18n.init({
    defaultNS: "components",
    fallbackLng: "en",
    interpolation: { escapeValue: false },
    lng: "en",
    ns: ["components"],
    resources: {
      en: {
        components: {
          limitedList: "+{{count}} more",
        },
      },
    },
  });
});

afterEach(() => {
  vi.restoreAllMocks();
});

describe("DagCard", () => {
  it("DagCard should render without tags", () => {
    renderCard(mockDag);
    expect(screen.getByText(mockDag.dag_display_name)).toBeInTheDocument();
    expect(screen.queryByTestId("dag-tag")).toBeNull();
  });

  it("DagCard should not show +X more text if there is only +1 over the limit", () => {
    const tags = [
      { dag_display_name: "id", dag_id: "id", name: "tag1" },
      { dag_display_name: "id", dag_id: "id", name: "tag2" },
      { dag_display_name: "id", dag_id: "id", name: "tag3" },
      { dag_display_name: "id", dag_id: "id", name: "tag4" },
    ] satisfies Array<DagTagResponse>;

    const expandedMockDag = {
      ...mockDag,
      tags,
    } satisfies DAGWithLatestDagRunsResponse;

    renderCard(expandedMockDag);
    expect(screen.getByTestId("dag-id")).toBeInTheDocument();
    expect(screen.getByTestId("dag-tag")).toBeInTheDocument();
    expect(screen.queryByText("tag3")).toBeInTheDocument();
    expect(screen.queryByText("tag4")).toBeInTheDocument();
    expect(screen.queryByText(", +1 more")).toBeNull();
  });

  it("DagCard should show +X more text if there are more than 3 tags", () => {
    const tags = [
      { dag_display_name: "id", dag_id: "id", name: "tag1" },
      { dag_display_name: "id", dag_id: "id", name: "tag2" },
      { dag_display_name: "id", dag_id: "id", name: "tag3" },
      { dag_display_name: "id", dag_id: "id", name: "tag4" },
      { dag_display_name: "id", dag_id: "id", name: "tag5" },
    ] satisfies Array<DagTagResponse>;

    const expandedMockDag = {
      ...mockDag,
      tags,
    } satisfies DAGWithLatestDagRunsResponse;

    renderCard(expandedMockDag);
    expect(screen.getByTestId("dag-id")).toBeInTheDocument();
    expect(screen.getByTestId("dag-tag")).toBeInTheDocument();
    expect(screen.getByText("+2 more")).toBeInTheDocument();
  });

  it("DagCard should render schedule section", () => {
    renderCard(mockDag);
    const scheduleElement = screen.getByTestId("schedule");

    expect(scheduleElement).toBeInTheDocument();
    // Should display the timetable summary from mockDag
    expect(scheduleElement).toHaveTextContent("* * * * *");
  });

  it("DagCard should render latest run section with actual run data", () => {
    renderCard(mockDag);
    const latestRunElement = screen.getByTestId("latest-run");

    expect(latestRunElement).toBeInTheDocument();
    // Should contain the formatted latest run timestamp (formatted for GMT timezone)
    expect(latestRunElement).toHaveTextContent("2025-09-19 19:22:00");
  });

  it("DagCard should share one tooltip controller across recent runs", async () => {
    vi.useFakeTimers();
    renderCard(mockDag);

    const recentRuns = screen.getAllByTestId("recent-run");
    const tooltipOwners = new Set(recentRuns.map((run) => run.getAttribute("data-ownedby")));
    const secondRecentRun = recentRuns.at(1);

    try {
      expect(recentRuns).toHaveLength(mockDag.latest_dag_runs.length);
      expect(tooltipOwners.size).toBe(1);
      expect(tooltipOwners.has(null)).toBe(false);
      expect(secondRecentRun).toBeDefined();

      if (secondRecentRun === undefined) {
        throw new Error("Expected at least two recent runs");
      }

      await act(async () => {
        fireEvent.focus(secondRecentRun);
        fireEvent.pointerEnter(secondRecentRun);
        await vi.advanceTimersByTimeAsync(500);
      });

      expect(screen.getByRole("tooltip")).toHaveTextContent("2025-09-19 19:21:00");
    } finally {
      vi.useRealTimers();
    }
  });

  it("DagCard should render next run section with timestamp", () => {
    renderCard(mockDag);
    const nextRunElement = screen.getByTestId("next-run");

    expect(nextRunElement).toBeInTheDocument();
    // Should display the formatted next run timestamp (converted to GMT timezone)
    expect(nextRunElement).toHaveTextContent("2024-08-22 19:00:00");
  });

  it("DagCard should not render next run timestamp for a paused Dag", () => {
    renderCard({ ...mockDag, is_paused: true });
    const nextRunElement = screen.getByTestId("next-run");

    expect(nextRunElement).toBeInTheDocument();
    expect(nextRunElement).not.toHaveTextContent("2024-08-22 19:00:00");
  });

  it("DagCard should render StateBadge as success", () => {
    renderCard(mockDag);
    const stateBadge = screen.getByTestId("state-badge");

    expect(stateBadge).toBeInTheDocument();
    // Should have the success state from mockDag.latest_dag_runs[0].state
    expect(stateBadge).toHaveAttribute("aria-label", "success");
  });

  it("DagCard should render StateBadge as failed", () => {
    const [firstDagRun] = mockDag.latest_dag_runs;

    if (!firstDagRun) {
      throw new Error("Mock data should have at least one dag run");
    }

    const mockDagWithFailedRun = {
      ...mockDag,
      latest_dag_runs: [
        {
          ...firstDagRun,
          state: "failed" as const,
        },
      ],
    } satisfies DAGWithLatestDagRunsResponse;

    renderCard(mockDagWithFailedRun);
    const stateBadge = screen.getByTestId("state-badge");

    expect(stateBadge).toBeInTheDocument();
    // Should have the failed state
    expect(stateBadge).toHaveAttribute("aria-label", "failed");
  });
});
