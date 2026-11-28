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
import type { PropsWithChildren } from "react";

import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import { beforeEach, describe, expect, it, vi } from "vitest";

import type * as OpenapiQueries from "openapi/queries";
import { DeadlinesService } from "openapi/requests/services.gen";
import type {
  DeadlineAlertResponse,
  DeadlineCollectionResponse,
  DeadlineResponse,
  DAGRunResponse,
  RunDeadlineAlertResponse,
} from "openapi/requests/types.gen";

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

const buildAlert = (
  filter: DeadlineAlertResponse["run_type_filter"],
  id: string,
  isApplicable: boolean,
): RunDeadlineAlertResponse => ({
  created_at: "2025-01-01T00:00:00Z",
  id,
  interval: 25,
  is_applicable: isApplicable,
  reference_type: "DagRunQueuedAtDeadline",
  run_type_filter: filter,
});

const buildDeadline = (alertId = "alert"): DeadlineResponse => ({
  alert_id: alertId,
  callback_id: "callback",
  callback_type: "triggerer",
  created_at: "2025-01-01T00:00:00Z",
  dag_id: baseDagRun.dag_id,
  dag_run_id: baseDagRun.dag_run_id,
  deadline_time: "2025-01-01T00:00:25Z",
  id: "deadline",
  missed: true,
});

const mockDeadlineData = (data: Partial<DeadlineCollectionResponse>) =>
  vi.mocked(useDeadlinesServiceGetDeadlines).mockReturnValue({
    data: { deadlines: [], total_entries: 0, ...data },
    isLoading: false,
  } as ReturnType<typeof useDeadlinesServiceGetDeadlines>);

const headerQuery = {
  dagId: baseDagRun.dag_id,
  dagRunId: baseDagRun.dag_run_id,
  includeAlerts: true,
  limit: 10,
  orderBy: ["deadline_time"],
};

describe("Header", () => {
  beforeEach(() => {
    mockConfig.multi_team = false;
    mockDeadlineData({});
    vi.mocked(useDeadlinesServiceGetDagDeadlineAlerts).mockClear();
  });

  it.each([
    { applicability: [false], visible: false },
    { applicability: [true], visible: true },
    { applicability: [false, false], visible: false },
    { applicability: [false, true], visible: true },
  ])("shows deadline status=$visible for API applicability $applicability", ({ applicability, visible }) => {
    mockDeadlineData({
      deadline_alerts: applicability.map((isApplicable, index) =>
        buildAlert(["scheduled"], String(index), isApplicable),
      ),
    });
    render(<Header dagRun={{ ...baseDagRun, run_type: "backfill" }} />, { wrapper: Wrapper });
    expect(Boolean(screen.queryByText(i18n.t("dag:deadlineStatus.label")))).toBe(visible);
    expect(Boolean(screen.queryByText(i18n.t("dag:deadlineStatus.met")))).toBe(visible);
    for (const [params] of vi.mocked(useDeadlinesServiceGetDeadlines).mock.calls) {
      expect(params).toEqual(headerQuery);
    }
    expect(useDeadlinesServiceGetDagDeadlineAlerts).not.toHaveBeenCalled();
  });

  it.each([
    { filter: ["scheduled"], isApplicable: false },
    { filter: ["backfill"], isApplicable: true },
    { filter: null, isApplicable: true },
  ] satisfies Array<{
    filter: DeadlineAlertResponse["run_type_filter"];
    isApplicable: boolean;
  }>)("shows stored deadlines for backfill with $filter", ({ filter, isApplicable }) => {
    mockDeadlineData({
      deadline_alerts: [buildAlert(filter, "alert", isApplicable)],
      deadlines: [buildDeadline()],
      total_entries: 1,
    });
    render(<Header dagRun={{ ...baseDagRun, run_type: "backfill" }} />, { wrapper: Wrapper });
    expect(screen.getByText(i18n.t("dag:deadlineStatus.missed"))).toBeInTheDocument();
  });

  it.each(["excluded", "included"])("shows stored %s history with mixed filters", (storedAlert) => {
    mockDeadlineData({
      deadline_alerts: [buildAlert(["scheduled"], "excluded", false), buildAlert(null, "included", true)],
      deadlines: [buildDeadline(storedAlert)],
      total_entries: 1,
    });
    render(<Header dagRun={{ ...baseDagRun, run_type: "backfill" }} />, { wrapper: Wrapper });
    expect(screen.getByText(i18n.t("dag:deadlineStatus.missed"))).toBeInTheDocument();
    expect(screen.queryByText(i18n.t("dag:deadlineStatus.met"))).not.toBeInTheDocument();
  });

  it("shows stored deadlines when creation-version definitions are unavailable", () => {
    mockDeadlineData({ deadline_alerts: [], deadlines: [buildDeadline("old")], total_entries: 1 });
    render(<Header dagRun={{ ...baseDagRun, run_type: "backfill" }} />, { wrapper: Wrapper });
    expect(screen.getByText(i18n.t("dag:deadlineStatus.missed"))).toBeInTheDocument();
  });

  it("shares one service request between the header and deadline status", async () => {
    const actual = await vi.importActual<typeof OpenapiQueries>("openapi/queries");
    const request = vi.spyOn(DeadlinesService, "getDeadlines").mockResolvedValue({
      deadline_alerts: [buildAlert(["scheduled"], "alert", false)],
      deadlines: [buildDeadline()],
      total_entries: 1,
    });

    vi.mocked(useDeadlinesServiceGetDeadlines).mockImplementation(actual.useDeadlinesServiceGetDeadlines);
    render(<Header dagRun={{ ...baseDagRun, run_type: "backfill" }} />, { wrapper: Wrapper });
    expect(await screen.findByText(i18n.t("dag:deadlineStatus.missed"))).toBeInTheDocument();
    expect(request).toHaveBeenCalledExactlyOnceWith(headerQuery);
    expect(useDeadlinesServiceGetDagDeadlineAlerts).not.toHaveBeenCalled();
    request.mockRestore();
  });

  it.each([
    { filter: ["backfill"], initialApplicable: false, nextApplicable: true },
    { filter: ["scheduled"], initialApplicable: true, nextApplicable: false },
  ] satisfies Array<{
    filter: DeadlineAlertResponse["run_type_filter"];
    initialApplicable: boolean;
    nextApplicable: boolean;
  }>)(
    "refreshes applicability from $initialApplicable to $nextApplicable when the same run becomes backfill",
    async ({ filter, initialApplicable, nextApplicable }) => {
      const queryClient = new QueryClient({
        defaultOptions: { queries: { retry: false, staleTime: 5 * 60 * 1000 } },
      });
      const StableWrapper = ({ children }: PropsWithChildren) => (
        <Wrapper>
          <QueryClientProvider client={queryClient}>{children}</QueryClientProvider>
        </Wrapper>
      );
      const actual = await vi.importActual<typeof OpenapiQueries>("openapi/queries");
      const initialData: DeadlineCollectionResponse = {
        deadline_alerts: [buildAlert(filter, "alert", initialApplicable)],
        deadlines: [],
        total_entries: 0,
      };
      const request = vi.spyOn(DeadlinesService, "getDeadlines").mockResolvedValue(initialData);

      vi.mocked(useDeadlinesServiceGetDeadlines).mockImplementation(actual.useDeadlinesServiceGetDeadlines);

      try {
        const dagRun = { ...baseDagRun, run_type: "scheduled" as const };
        const { rerender } = render(<Header dagRun={dagRun} />, { wrapper: StableWrapper });

        await waitFor(() => {
          expect(request).toHaveBeenCalledTimes(1);
          expect(queryClient.isFetching()).toBe(0);
          expect(Boolean(screen.queryByText(i18n.t("dag:deadlineStatus.met")))).toBe(initialApplicable);
        });

        rerender(<Header dagRun={{ ...dagRun, duration: 42 }} />);
        expect(request).toHaveBeenCalledTimes(1);

        request.mockResolvedValue({
          ...initialData,
          deadline_alerts: [buildAlert(filter, "alert", nextApplicable)],
        });
        rerender(<Header dagRun={{ ...dagRun, run_type: "backfill" }} />);

        await waitFor(() => {
          expect(request).toHaveBeenCalledTimes(2);
          expect(queryClient.isFetching()).toBe(0);
          expect(Boolean(screen.queryByText(i18n.t("dag:deadlineStatus.label")))).toBe(nextApplicable);
          expect(Boolean(screen.queryByText(i18n.t("dag:deadlineStatus.met")))).toBe(nextApplicable);
        });
        for (const [params] of request.mock.calls) {
          expect(params).toEqual(headerQuery);
        }
        expect(useDeadlinesServiceGetDagDeadlineAlerts).not.toHaveBeenCalled();
      } finally {
        queryClient.clear();
        request.mockRestore();
      }
    },
  );

  it("keeps alert metadata on every modal page without requesting it again", async () => {
    const alerts = [buildAlert(["scheduled"], "excluded", false), buildAlert(["backfill"], "included", true)];
    const records = [
      ...Array.from({ length: 12 }, (_, index) => ({
        ...buildDeadline("excluded"),
        alert_name: "excluded_history",
        id: `excluded_${index}`,
      })),
      ...Array.from({ length: 11 }, (_, index) => ({
        ...buildDeadline("included"),
        alert_name: `included_${index}`,
        id: `included_${index}`,
        missed: false,
      })),
    ];

    vi.mocked(useDeadlinesServiceGetDeadlines).mockImplementation(
      ({ includeAlerts, limit = 10, offset = 0 }) =>
        ({
          data: {
            deadlines: records.slice(offset, offset + limit),
            total_entries: records.length,
            ...(includeAlerts ? { deadline_alerts: alerts } : {}),
          },
          isLoading: false,
        }) as ReturnType<typeof useDeadlinesServiceGetDeadlines>,
    );
    render(<Header dagRun={{ ...baseDagRun, run_type: "backfill" }} />, { wrapper: Wrapper });
    fireEvent.click(
      screen.getByRole("button", { name: i18n.t("dag:deadlineStatus.missedCount", { count: 10 }) }),
    );
    const dialog = await screen.findByRole("dialog");

    expect(within(dialog).getAllByText("excluded_history")).toHaveLength(10);
    fireEvent.click(within(dialog).getByRole("button", { name: "next page" }));
    await waitFor(() => expect(within(dialog).getByText("included_0")).toBeInTheDocument());
    expect(within(dialog).getAllByText("excluded_history")).toHaveLength(2);
    fireEvent.click(within(dialog).getByRole("button", { name: "next page" }));
    await waitFor(() => expect(within(dialog).getByText("included_10")).toBeInTheDocument());
    expect(within(dialog).queryByText("excluded_history")).not.toBeInTheDocument();
    expect(
      within(dialog).getAllByText(
        i18n.t("dag:deadlineAlerts.completionRule", {
          interval: "25 seconds",
          reference: i18n.t("dag:deadlineAlerts.referenceType.DagRunQueuedAtDeadline"),
        }),
      ),
    ).toHaveLength(3);
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
    expect(useDeadlinesServiceGetDagDeadlineAlerts).not.toHaveBeenCalled();
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
