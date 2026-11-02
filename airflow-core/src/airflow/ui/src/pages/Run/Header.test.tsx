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
import { fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import { beforeEach, describe, expect, it, vi } from "vitest";

import type * as OpenapiQueries from "openapi/queries";
import type { DAGRunResponse } from "openapi/requests/types.gen";

import i18n from "src/i18n/config";
import { Wrapper } from "src/utils/Wrapper";

import { Header } from "./Header";

// Action buttons and note preview pull in mutation/permission wiring that is
// unrelated to the partition key label under test; stub them out so the test
// only depends on the stats rendered by Header itself.
vi.mock("src/components/Clear", () => ({ ClearRunButton: () => undefined }));
vi.mock("src/components/MarkAs", () => ({ MarkRunAsButton: () => undefined }));
vi.mock("src/components/NeedsReviewButton", () => ({ NeedsReviewButtonWithModal: () => undefined }));
vi.mock("src/pages/DagRuns/DeleteRunButton", () => ({ default: () => undefined }));
vi.mock("src/components/NotePreview", () => ({ NotePreview: () => undefined }));

vi.mock("openapi/queries", async (importOriginal) => {
  const actual = await importOriginal<typeof OpenapiQueries>();

  return {
    ...actual,
    useDeadlinesServiceGetDagDeadlineAlerts: vi.fn(),
    useDeadlinesServiceGetDeadlines: vi.fn(),
  };
});

const mockConfig: Record<string, unknown> = { multi_team: false };

vi.mock("src/queries/useConfig", () => ({
  useConfig: (key: string) => mockConfig[key],
}));

const { useDeadlinesServiceGetDagDeadlineAlerts, useDeadlinesServiceGetDeadlines } =
  await import("openapi/queries");

const baseDagRun = {
  bundle_version: null,
  conf: {},
  dag_display_name: "test_dag",
  dag_id: "test_dag",
  dag_run_id: "run_1",
  dag_versions: [],
  data_interval_end: null,
  data_interval_start: null,
  duration: null,
  end_date: null,
  last_scheduling_decision: null,
  logical_date: null,
  note: null,
  partition_date: null,
  queued_at: null,
  run_after: "2025-01-01T00:00:00Z",
  run_type: "manual",
  start_date: null,
  state: "success",
  triggered_by: null,
  triggering_user_name: null,
} satisfies Partial<DAGRunResponse> as unknown as DAGRunResponse;

describe("Header", () => {
  beforeEach(() => {
    mockConfig.multi_team = false;
    vi.mocked(useDeadlinesServiceGetDeadlines).mockReturnValue({
      data: { deadlines: [], total_entries: 0 },
      isLoading: false,
    } as unknown as ReturnType<typeof useDeadlinesServiceGetDeadlines>);
    vi.mocked(useDeadlinesServiceGetDagDeadlineAlerts).mockReturnValue({
      data: undefined,
    } as ReturnType<typeof useDeadlinesServiceGetDagDeadlineAlerts>);
  });

  it.each([
    { filters: [["scheduled"]], runType: "backfill", visible: false },
    { filters: [["scheduled"]], runType: "scheduled", visible: true },
    { filters: [null], runType: "backfill", visible: true },
    { filters: [["scheduled"], ["manual"]], runType: "backfill", visible: false },
    { filters: [["scheduled"], ["backfill"]], runType: "backfill", visible: true },
  ] as const)(
    "shows deadline status=$visible for $runType with $filters",
    ({ filters, runType, visible }) => {
      vi.mocked(useDeadlinesServiceGetDagDeadlineAlerts).mockReturnValue({
        data: {
          deadline_alerts: filters.map((filter, index) => ({
            id: String(index),
            interval: 25,
            reference_type: "DagRunQueuedAtDeadline",
            run_type_filter: filter,
          })),
          total_entries: filters.length,
        },
        isLoading: false,
      } as unknown as ReturnType<typeof useDeadlinesServiceGetDagDeadlineAlerts>);

      render(<Header dagRun={{ ...baseDagRun, run_type: runType }} />, { wrapper: Wrapper });

      expect(screen.queryByText(i18n.t("dag:deadlineStatus.label")))[
        visible ? "toBeInTheDocument" : "toBeNull"
      ]();
      expect(screen.queryByText(i18n.t("dag:deadlineStatus.met")))[
        visible ? "toBeInTheDocument" : "toBeNull"
      ]();
    },
  );

  it.each([
    { creationFilter: ["scheduled"], latestFilter: ["backfill"], visible: false },
    { creationFilter: ["backfill"], latestFilter: ["scheduled"], visible: true },
  ])(
    "preserves deadline status=$visible after the filter changes",
    ({ creationFilter, latestFilter, visible }) => {
      vi.mocked(useDeadlinesServiceGetDagDeadlineAlerts).mockImplementation(
        ({ dagRunId }) =>
          ({
            data: {
              deadline_alerts: [
                {
                  id: "alert",
                  interval: 25,
                  reference_type: "DagRunQueuedAtDeadline",
                  run_type_filter: dagRunId === baseDagRun.dag_run_id ? creationFilter : latestFilter,
                },
              ],
              total_entries: 1,
            },
            isLoading: false,
          }) as ReturnType<typeof useDeadlinesServiceGetDagDeadlineAlerts>,
      );

      render(<Header dagRun={{ ...baseDagRun, run_type: "backfill" }} />, { wrapper: Wrapper });

      expect(screen.queryByText(i18n.t("dag:deadlineStatus.label")))[
        visible ? "toBeInTheDocument" : "toBeNull"
      ]();
      expect(screen.queryByText(i18n.t("dag:deadlineStatus.met")))[
        visible ? "toBeInTheDocument" : "toBeNull"
      ]();
      for (const [params] of vi.mocked(useDeadlinesServiceGetDagDeadlineAlerts).mock.calls) {
        expect(params).toEqual({ dagId: baseDagRun.dag_id, dagRunId: baseDagRun.dag_run_id, limit: 100 });
      }
    },
  );

  it.each([{ filter: ["scheduled"] }, { filter: ["backfill"] }, { filter: null }])(
    "shows stored deadlines for backfill with $filter",
    ({ filter }) => {
      vi.mocked(useDeadlinesServiceGetDagDeadlineAlerts).mockReturnValue({
        data: {
          deadline_alerts: [
            {
              id: "alert",
              interval: 25,
              reference_type: "DagRunQueuedAtDeadline",
              run_type_filter: filter,
            },
          ],
          total_entries: 1,
        },
        isLoading: false,
      } as unknown as ReturnType<typeof useDeadlinesServiceGetDagDeadlineAlerts>);
      vi.mocked(useDeadlinesServiceGetDeadlines).mockReturnValue({
        data: {
          deadlines: [
            { alert_id: "alert", deadline_time: "2025-01-01T00:00:25Z", id: "deadline", missed: true },
          ],
          total_entries: 1,
        },
        isLoading: false,
      } as unknown as ReturnType<typeof useDeadlinesServiceGetDeadlines>);

      render(<Header dagRun={{ ...baseDagRun, run_type: "backfill" }} />, { wrapper: Wrapper });

      expect(screen.getByText(i18n.t("dag:deadlineStatus.label"))).toBeInTheDocument();
      expect(screen.getByText(i18n.t("dag:deadlineStatus.missed"))).toBeInTheDocument();
    },
  );

  it.each(["excluded", "included"])("shows stored %s history with mixed run type filters", (storedAlert) => {
    vi.mocked(useDeadlinesServiceGetDagDeadlineAlerts).mockReturnValue({
      data: {
        deadline_alerts: [
          {
            id: "excluded",
            interval: 25,
            reference_type: "DagRunQueuedAtDeadline",
            run_type_filter: ["scheduled"],
          },
          { id: "included", interval: 3600, reference_type: "DagRunQueuedAtDeadline", run_type_filter: null },
        ],
        total_entries: 2,
      },
      isLoading: false,
    } as unknown as ReturnType<typeof useDeadlinesServiceGetDagDeadlineAlerts>);
    vi.mocked(useDeadlinesServiceGetDeadlines).mockReturnValue({
      data: {
        deadlines: [
          { alert_id: storedAlert, deadline_time: "2025-01-01T00:00:25Z", id: "deadline", missed: true },
        ],
        total_entries: 1,
      },
      isLoading: false,
    } as unknown as ReturnType<typeof useDeadlinesServiceGetDeadlines>);
    render(<Header dagRun={{ ...baseDagRun, run_type: "backfill" }} />, { wrapper: Wrapper });
    expect(screen.getByText(i18n.t("dag:deadlineStatus.missed"))).toBeInTheDocument();
    expect(screen.queryByText(i18n.t("dag:deadlineStatus.met"))).not.toBeInTheDocument();
    expect(useDeadlinesServiceGetDeadlines).toHaveBeenCalledWith({
      dagId: baseDagRun.dag_id,
      dagRunId: baseDagRun.dag_run_id,
      limit: 10,
      orderBy: ["deadline_time"],
    });
  });

  it("shows stored deadlines when the run's alert definitions are unavailable", () => {
    vi.mocked(useDeadlinesServiceGetDeadlines).mockReturnValue({
      data: {
        deadlines: [{ alert_id: "old", deadline_time: "2025-01-01T00:00:25Z", id: "deadline", missed: true }],
        total_entries: 1,
      },
      isLoading: false,
    } as unknown as ReturnType<typeof useDeadlinesServiceGetDeadlines>);
    render(<Header dagRun={{ ...baseDagRun, run_type: "backfill" }} />, { wrapper: Wrapper });
    expect(screen.getByText(i18n.t("dag:deadlineStatus.label"))).toBeInTheDocument();
    expect(screen.getByText(i18n.t("dag:deadlineStatus.missed"))).toBeInTheDocument();
  });

  it("shows all stored history in the header and across modal pages", async () => {
    vi.mocked(useDeadlinesServiceGetDagDeadlineAlerts).mockReturnValue({
      data: {
        deadline_alerts: [
          {
            id: "excluded",
            interval: 25,
            reference_type: "DagRunQueuedAtDeadline",
            run_type_filter: ["scheduled"],
          },
          {
            id: "included",
            interval: 3600,
            reference_type: "DagRunQueuedAtDeadline",
            run_type_filter: ["backfill"],
          },
        ],
        total_entries: 2,
      },
      isLoading: false,
    } as unknown as ReturnType<typeof useDeadlinesServiceGetDagDeadlineAlerts>);
    const records = [
      ...Array.from({ length: 12 }, (_, index) => ({
        alert_id: "excluded",
        alert_name: "excluded_history",
        deadline_time: "2025-01-01T00:00:25Z",
        id: `excluded_${index}`,
        missed: true,
      })),
      ...Array.from({ length: 11 }, (_, index) => ({
        alert_id: "included",
        alert_name: `included_${index}`,
        deadline_time: "2025-01-02T00:00:25Z",
        id: `included_${index}`,
        missed: false,
      })),
    ];

    vi.mocked(useDeadlinesServiceGetDeadlines).mockImplementation(
      ({ limit = 10, offset = 0 }) =>
        ({
          data: { deadlines: records.slice(offset, offset + limit), total_entries: records.length },
          isLoading: false,
        }) as unknown as ReturnType<typeof useDeadlinesServiceGetDeadlines>,
    );

    render(<Header dagRun={{ ...baseDagRun, run_type: "backfill" }} />, { wrapper: Wrapper });

    fireEvent.click(
      screen.getByRole("button", { name: i18n.t("dag:deadlineStatus.missedCount", { count: 10 }) }),
    );
    const dialog = await screen.findByRole("dialog");

    expect(within(dialog).getAllByText("excluded_history")).toHaveLength(10);
    expect(within(dialog).queryByText("included_0")).not.toBeInTheDocument();
    fireEvent.click(within(dialog).getByRole("button", { name: "next page" }));
    await waitFor(() => expect(within(dialog).getByText("included_0")).toBeInTheDocument());
    expect(within(dialog).getAllByText("excluded_history")).toHaveLength(2);
    fireEvent.click(within(dialog).getByRole("button", { name: "next page" }));
    await waitFor(() => expect(within(dialog).getByText("included_10")).toBeInTheDocument());
    expect(within(dialog).queryByText("excluded_history")).not.toBeInTheDocument();
    expect(useDeadlinesServiceGetDeadlines).toHaveBeenCalledWith(
      {
        dagId: baseDagRun.dag_id,
        dagRunId: baseDagRun.dag_run_id,
        limit: 10,
        offset: 20,
        orderBy: ["deadline_time"],
      },
      undefined,
      { enabled: true },
    );
  });

  it("shows the partition key stat using the dagRun.partitionKey label when partition_key is set", () => {
    render(<Header dagRun={{ ...baseDagRun, partition_key: "2025-01-01" }} />, { wrapper: Wrapper });

    expect(screen.getByText(i18n.t("dagRun.partitionKey"))).toBeInTheDocument();
    expect(screen.getByText("2025-01-01")).toBeInTheDocument();
    expect(screen.queryByText(/mappedPartitionKey/iu)).not.toBeInTheDocument();
  });

  it("hides the partition key stat when partition_key is null", () => {
    render(<Header dagRun={{ ...baseDagRun, partition_key: null }} />, { wrapper: Wrapper });

    expect(screen.queryByText(i18n.t("dagRun.partitionKey"))).not.toBeInTheDocument();
  });

  it("shows the team stat when multi-team is enabled and a team is resolved", () => {
    mockConfig.multi_team = true;
    render(<Header dagRun={{ ...baseDagRun, team_name: "team-a" }} />, { wrapper: Wrapper });

    expect(screen.getByText(i18n.t("common:dagDetails.team"))).toBeInTheDocument();
    expect(screen.getByRole("link", { name: "team-a" })).toHaveAttribute("href", "/dags?teams=team-a");
  });

  it("hides the team stat when multi-team is disabled", () => {
    render(<Header dagRun={{ ...baseDagRun, team_name: "team-a" }} />, { wrapper: Wrapper });

    expect(screen.queryByText(i18n.t("common:dagDetails.team"))).not.toBeInTheDocument();
  });
});
