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
import { fireEvent, render, screen, waitForElementToBeRemoved, within } from "@testing-library/react";
import type * as ReactRouterDom from "react-router-dom";
import { beforeEach, describe, expect, it, vi } from "vitest";

import type { BackfillDagRunResponse, BackfillResponse } from "openapi/requests/types.gen";
import type * as Utils from "src/utils";
import { Wrapper } from "src/utils/Wrapper";

import { Backfills } from "./Backfills";

const mocks = vi.hoisted(() => ({
  listBackfillDagRuns: vi.fn(),
  listBackfills: vi.fn(),
}));

vi.mock("openapi/queries", () => ({
  useBackfillServiceListBackfillDagRuns: mocks.listBackfillDagRuns,
  useBackfillServiceListBackfillsUi: mocks.listBackfills,
}));

vi.mock("react-i18next", () => ({
  useTranslation: () => ({
    // eslint-disable-next-line id-length
    t: (key: string) =>
      ({
        "common:backfill_one": "Backfill",
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

  return { ...actual, useParams: () => ({ dagId: "example_dag" }) };
});

vi.mock("src/components/Time", () => ({
  default: ({ datetime }: { readonly datetime: string | null }) => <span>{datetime}</span>,
}));

vi.mock("src/queries/useConfig", () => ({
  useConfig: (key: string) => (key === "fallback_page_limit" ? 25 : undefined),
}));

vi.mock("src/utils", async (importOriginal) => {
  const actual = await importOriginal<typeof Utils>();

  return { ...actual, useAutoRefresh: () => 5000 };
});

const makeBackfill = (overrides: Partial<BackfillResponse> = {}): BackfillResponse => ({
  completed_at: null,
  created_at: "2026-06-30T00:00:00Z",
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
  ...overrides,
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

describe("Backfills", () => {
  beforeEach(() => {
    mocks.listBackfillDagRuns.mockReset();
    mocks.listBackfills.mockReset();
  });

  it("opens a backfill's associated slots in a dialog", async () => {
    const backfills = [
      makeBackfill(),
      makeBackfill({
        completed_at: "2026-08-06T00:00:00Z",
        from_date: "2026-08-01T00:00:00Z",
        id: 8,
        to_date: "2026-08-05T00:00:00Z",
      }),
    ];

    mocks.listBackfillDagRuns.mockReturnValue({
      data: { backfill_dag_runs: dagRuns, total_entries: dagRuns.length },
      error: undefined,
      isFetching: false,
      isLoading: false,
    });
    mocks.listBackfills.mockReturnValue({
      data: { backfills, total_entries: backfills.length },
      error: undefined,
      isFetching: false,
      isLoading: false,
    });

    render(<Backfills />, { wrapper: Wrapper });

    expect(screen.queryByRole("dialog")).not.toBeInTheDocument();

    fireEvent.click(screen.getByText("2026-07-01T00:00:00Z"));

    const dialog = await screen.findByRole("dialog");

    expect(within(dialog).getByRole("heading", { name: "Backfill #7" })).toBeInTheDocument();
    expect(within(dialog).getByRole("heading", { name: "2 Slots" })).toBeInTheDocument();
    expect(within(dialog).getByText("partition-a")).toBeInTheDocument();
    expect(within(dialog).getByText("Already exists")).toBeInTheDocument();
    expect(within(dialog).getByText("Success")).toBeInTheDocument();
    expect(within(dialog).getByRole("link", { name: "scheduled__2026-07-02" })).toHaveAttribute(
      "href",
      "/dags/example_dag/runs/scheduled__2026-07-02",
    );
    expect(mocks.listBackfillDagRuns).toHaveBeenLastCalledWith(
      {
        backfillId: 7,
        limit: 25,
        offset: 0,
      },
      undefined,
      {
        enabled: true,
        refetchInterval: 5000,
      },
    );
    expect(mocks.listBackfills).toHaveBeenCalledWith({
      dagId: "example_dag",
      limit: 25,
      offset: 0,
    });

    fireEvent.click(within(dialog).getByRole("button", { name: "Close" }));
    await waitForElementToBeRemoved(dialog);
    fireEvent.click(screen.getByText("2026-08-01T00:00:00Z"));
    await screen.findByRole("dialog");

    expect(mocks.listBackfillDagRuns).toHaveBeenLastCalledWith(
      {
        backfillId: 8,
        limit: 25,
        offset: 0,
      },
      undefined,
      {
        enabled: true,
        refetchInterval: false,
      },
    );
  });
});
