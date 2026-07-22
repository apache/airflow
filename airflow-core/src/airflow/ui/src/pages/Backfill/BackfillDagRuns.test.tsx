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
import { render, screen, within } from "@testing-library/react";
import type * as ReactRouterDom from "react-router-dom";
import { beforeEach, describe, expect, it, vi } from "vitest";

import type { BackfillDagRunResponse, BackfillResponse } from "openapi/requests/types.gen";
import type * as Utils from "src/utils";
import { Wrapper } from "src/utils/Wrapper";

import { BackfillDagRuns } from "./BackfillDagRuns";

const mocks = vi.hoisted(() => ({
  useBackfillServiceListBackfillDagRuns: vi.fn(),
  useOutletContext: vi.fn(),
  useParams: vi.fn(),
}));

vi.mock("openapi/queries", () => ({
  useBackfillServiceListBackfillDagRuns: mocks.useBackfillServiceListBackfillDagRuns,
}));

vi.mock("react-i18next", () => ({
  useTranslation: () => ({
    // eslint-disable-next-line id-length
    t: (key: string) =>
      ({
        "common:slot": "Slots",
        "components:backfill.exceptionReason.alreadyExists": "Already exists",
        "components:backfill.notCreatedReason": "Not Created Reason",
        dagRunState: "Dag Run State",
        "states.success": "Success",
      })[key] ?? key,
  }),
}));

vi.mock("react-router-dom", async (importOriginal) => {
  const actual = await importOriginal<typeof ReactRouterDom>();

  return { ...actual, useOutletContext: mocks.useOutletContext, useParams: mocks.useParams };
});

vi.mock("src/queries/useConfig", () => ({
  useConfig: (key: string) => (key === "fallback_page_limit" ? 25 : undefined),
}));

vi.mock("src/utils", async (importOriginal) => {
  const actual = await importOriginal<typeof Utils>();

  return { ...actual, useAutoRefresh: () => 5000 };
});

const dagRuns: Array<BackfillDagRunResponse> = [
  {
    backfill_id: 7,
    dag_id: "example_dag",
    dag_run_id: null,
    dag_run_state: null,
    exception_reason: "already exists",
    id: 1,
    logical_date: null,
    partition_key: "partition-a",
    sort_ordinal: 1,
  },
  {
    backfill_id: 7,
    dag_id: "example_dag",
    dag_run_id: "scheduled__2026-07-02",
    dag_run_state: "success",
    exception_reason: null,
    id: 2,
    logical_date: "2026-07-02T00:00:00Z",
    partition_key: null,
    sort_ordinal: 2,
  },
];

const backfill: BackfillResponse = {
  completed_at: null,
  created_at: "2026-07-01T00:00:00Z",
  dag_display_name: "Example Dag",
  dag_id: "example_dag",
  dag_run_conf: null,
  from_date: "2026-07-01T00:00:00Z",
  id: 7,
  is_paused: false,
  max_active_runs: 4,
  reprocess_behavior: "failed",
  to_date: "2026-07-05T00:00:00Z",
  updated_at: "2026-07-01T00:00:00Z",
};

describe("BackfillDagRuns", () => {
  beforeEach(() => {
    mocks.useBackfillServiceListBackfillDagRuns.mockReset();
    mocks.useOutletContext.mockReturnValue(backfill);
    mocks.useParams.mockReturnValue({ backfillId: "7", dagId: "example_dag" });
  });

  it("renders Dag run state and not-created reason in separate columns", () => {
    mocks.useBackfillServiceListBackfillDagRuns.mockReturnValue({
      data: { backfill_dag_runs: dagRuns, total_entries: dagRuns.length },
      error: undefined,
      isFetching: false,
      isLoading: false,
    });

    render(<BackfillDagRuns />, { wrapper: Wrapper });

    expect(screen.getByRole("heading", { name: "2 Slots" })).toBeInTheDocument();
    expect(screen.getByRole("columnheader", { name: "Dag Run State" })).toBeInTheDocument();
    expect(screen.getByRole("columnheader", { name: "Not Created Reason" })).toBeInTheDocument();

    const notCreatedRow = screen.getByText("partition-a").closest("tr");

    expect(notCreatedRow).not.toBeNull();
    expect(
      within(notCreatedRow as HTMLElement)
        .getAllByRole("cell")
        .map((cell) => cell.textContent),
    ).toEqual(["partition-a", "—", "Already exists", "—", "1"]);

    expect(screen.getByText("partition-a")).toBeInTheDocument();
    expect(screen.getByText("Already exists")).toBeInTheDocument();
    expect(screen.getByText("Success")).toBeInTheDocument();
    expect(screen.getByRole("link", { name: "scheduled__2026-07-02" })).toHaveAttribute(
      "href",
      "/dags/example_dag/runs/scheduled__2026-07-02",
    );
  });

  it("passes URL pagination to the API request", () => {
    mocks.useBackfillServiceListBackfillDagRuns.mockReturnValue({
      data: { backfill_dag_runs: [], total_entries: 0 },
      error: undefined,
      isFetching: false,
      isLoading: false,
    });

    render(<BackfillDagRuns />, { wrapper: Wrapper });

    expect(mocks.useBackfillServiceListBackfillDagRuns).toHaveBeenCalledWith(
      {
        backfillId: 7,
        limit: 25,
        offset: 0,
      },
      undefined,
      { enabled: true, refetchInterval: 5000 },
    );
  });

  it("stops polling when the backfill is completed", () => {
    mocks.useOutletContext.mockReturnValue({ ...backfill, completed_at: "2026-07-02T00:00:00Z" });
    mocks.useBackfillServiceListBackfillDagRuns.mockReturnValue({
      data: { backfill_dag_runs: [], total_entries: 0 },
      error: undefined,
      isFetching: false,
      isLoading: false,
    });

    render(<BackfillDagRuns />, { wrapper: Wrapper });

    expect(mocks.useBackfillServiceListBackfillDagRuns).toHaveBeenCalledWith(
      expect.anything(),
      undefined,
      expect.objectContaining({ refetchInterval: false }),
    );
  });

  it("does not request a malformed backfill ID", () => {
    mocks.useParams.mockReturnValue({ backfillId: "invalid", dagId: "example_dag" });
    mocks.useBackfillServiceListBackfillDagRuns.mockReturnValue({
      data: undefined,
      error: undefined,
      isFetching: false,
      isLoading: false,
    });

    render(<BackfillDagRuns />, { wrapper: Wrapper });

    expect(mocks.useBackfillServiceListBackfillDagRuns).toHaveBeenCalledWith(
      expect.objectContaining({ backfillId: 0 }),
      undefined,
      expect.objectContaining({ enabled: false }),
    );
  });
});
