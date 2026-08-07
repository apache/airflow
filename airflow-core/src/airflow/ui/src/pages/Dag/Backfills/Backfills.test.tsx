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
import {
  fireEvent,
  render,
  screen,
  waitFor,
  waitForElementToBeRemoved,
  within,
} from "@testing-library/react";
import { MemoryRouter, Route, Routes } from "react-router-dom";
import { beforeEach, describe, expect, it, vi } from "vitest";

import type { BackfillDagRunResponse, BackfillResponse } from "openapi/requests/types.gen";
import type * as Utils from "src/utils";
import { BaseWrapper } from "src/utils/Wrapper";

import { Backfills } from "./Backfills";

const mocks = vi.hoisted(() => ({
  getBackfill: vi.fn(),
  listBackfillDagRuns: vi.fn(),
  listBackfills: vi.fn(),
}));

vi.mock("openapi/queries", () => ({
  useBackfillServiceGetBackfill: mocks.getBackfill,
  useBackfillServiceListBackfillDagRuns: mocks.listBackfillDagRuns,
  useBackfillServiceListBackfillsUi: mocks.listBackfills,
}));

vi.mock("react-i18next", () => ({
  useTranslation: () => ({
    // eslint-disable-next-line id-length
    t: (key: string, options?: { id?: number }) =>
      key === "components:backfill.viewSlots"
        ? `View slots for Backfill #${options?.id}`
        : ({
            "common:backfill_one": "Backfill",
            "common:slot": "Slots",
            "components:backfill.exceptionReason.alreadyExists": "Already exists",
            "components:backfill.exceptionReason.inFlight": "In flight",
            "components:backfill.exceptionReason.unknown": "Unknown",
            "components:backfill.notCreatedReason": "Not Created Reason",
            "dagRun.partitionKey": "Partition Key",
            dagRunState: "Dag Run State",
            logicalDate: "Logical Date",
            "states.success": "Success",
          }[key] ?? key),
  }),
}));

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
    logical_date: null,
    partition_key: "partition-b",
    sort_ordinal: 2,
  },
];

const renderBackfills = (initialEntry = "/dags/example_dag/backfills") =>
  render(
    <BaseWrapper>
      <MemoryRouter initialEntries={[initialEntry]}>
        <Routes>
          <Route element={<Backfills />} path="/dags/:dagId/backfills" />
          <Route element={<Backfills />} path="/dags/:dagId/backfills/:backfillId" />
        </Routes>
      </MemoryRouter>
    </BaseWrapper>,
  );

const expectGetBackfillQuery = (backfillId: number) => {
  const [parameters, queryKey, options] = mocks.getBackfill.mock.lastCall as [
    { backfillId: number },
    undefined,
    {
      enabled: boolean;
      refetchInterval: (query: { state: { data: BackfillResponse } }) => number | false;
    },
  ];

  expect(parameters).toEqual({ backfillId });
  expect(queryKey).toBeUndefined();
  expect(options.enabled).toBe(true);
  expect(typeof options.refetchInterval).toBe("function");

  return options.refetchInterval;
};

const expectDagRunsQuery = (backfillId: number) => {
  const [parameters, queryKey, options] = mocks.listBackfillDagRuns.mock.lastCall as [
    { backfillId: number; limit: number; offset: number },
    undefined,
    {
      enabled: boolean;
      refetchInterval: (query: {
        state: { data: { backfill_dag_runs: Array<BackfillDagRunResponse> } };
      }) => number | false;
    },
  ];

  expect(parameters).toEqual({ backfillId, limit: 25, offset: 0 });
  expect(queryKey).toBeUndefined();
  expect(options.enabled).toBe(true);
  expect(typeof options.refetchInterval).toBe("function");

  return options.refetchInterval;
};

describe("Backfills", () => {
  beforeEach(() => {
    mocks.getBackfill.mockReset();
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

    mocks.getBackfill.mockImplementation(({ backfillId }: { backfillId: number }) => ({
      data: backfillId === 8 ? backfills[1] : backfills[0],
      error: undefined,
      isLoading: false,
    }));
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

    renderBackfills();

    expect(screen.queryByRole("dialog")).not.toBeInTheDocument();

    fireEvent.click(screen.getByRole("button", { name: "View slots for Backfill #7" }));

    const dialog = await screen.findByRole("dialog");

    expect(within(dialog).getByRole("heading", { name: "Backfill #7" })).toBeInTheDocument();
    expect(within(dialog).getByRole("heading", { name: "2 Slots" })).toBeInTheDocument();
    expect(within(dialog).getByRole("columnheader", { name: "Partition Key" })).toBeInTheDocument();
    expect(within(dialog).getByText("partition-a")).toBeInTheDocument();
    expect(within(dialog).getByText("Already exists")).toBeInTheDocument();
    expect(within(dialog).getByText("Success")).toBeInTheDocument();
    expect(within(dialog).getByRole("link", { name: "scheduled__2026-07-02" })).toHaveAttribute(
      "href",
      "/dags/example_dag/runs/scheduled__2026-07-02",
    );
    const getDagRunsRefetchInterval = expectDagRunsQuery(7);

    expect(getDagRunsRefetchInterval({ state: { data: { backfill_dag_runs: dagRuns } } })).toBe(5000);
    const getBackfillRefetchInterval = expectGetBackfillQuery(7);

    expect(getBackfillRefetchInterval({ state: { data: makeBackfill() } })).toBe(5000);
    expect(
      getBackfillRefetchInterval({
        state: { data: makeBackfill({ completed_at: "2026-08-06T00:00:00Z" }) },
      }),
    ).toBe(false);
    expect(mocks.listBackfills).toHaveBeenCalledWith({
      dagId: "example_dag",
      limit: 25,
      offset: 0,
    });

    fireEvent.click(within(dialog).getByRole("button", { name: "Close" }));
    await waitForElementToBeRemoved(dialog);
    fireEvent.click(screen.getByText("2026-08-01T00:00:00Z"));
    await screen.findByRole("dialog");

    const getCompletedDagRunsRefetchInterval = expectDagRunsQuery(8);
    const [, completedDagRun] = dagRuns;

    if (completedDagRun === undefined) {
      throw new Error("Expected a completed Dag run fixture");
    }

    expect(getCompletedDagRunsRefetchInterval({ state: { data: { backfill_dag_runs: dagRuns } } })).toBe(
      false,
    );
    expect(
      getCompletedDagRunsRefetchInterval({
        state: {
          data: {
            backfill_dag_runs: [
              {
                ...completedDagRun,
                dag_run_state: "queued",
              },
            ],
          },
        },
      }),
    ).toBe(5000);
  });

  it("opens a linked backfill with logical dates and renders creation reasons", async () => {
    const backfill = makeBackfill({ id: 9 });
    const logicalDate = "2026-07-03T00:00:00Z";
    const reason = "future reason";

    mocks.getBackfill.mockReturnValue({
      data: backfill,
      error: undefined,
      isLoading: false,
    });
    mocks.listBackfillDagRuns.mockReturnValue({
      data: {
        backfill_dag_runs: [
          {
            ...dagRuns[0],
            backfill_id: 9,
            exception_reason: "in flight",
            logical_date: logicalDate,
            partition_key: null,
          },
          {
            ...dagRuns[0],
            backfill_id: 9,
            exception_reason: "unknown",
            id: 2,
            logical_date: logicalDate,
            partition_key: null,
            sort_ordinal: 2,
          },
          {
            ...dagRuns[0],
            backfill_id: 9,
            exception_reason: reason as BackfillDagRunResponse["exception_reason"],
            id: 3,
            logical_date: logicalDate,
            partition_key: null,
            sort_ordinal: 3,
          },
        ],
        total_entries: 3,
      },
      error: undefined,
      isFetching: false,
      isLoading: false,
    });
    mocks.listBackfills.mockReturnValue({
      data: { backfills: [], total_entries: 0 },
      error: undefined,
      isFetching: false,
      isLoading: false,
    });

    renderBackfills("/dags/example_dag/backfills/9");

    const dialog = await screen.findByRole("dialog");

    expect(within(dialog).getByRole("columnheader", { name: "Logical Date" })).toBeInTheDocument();
    expect(within(dialog).getAllByText(logicalDate)).toHaveLength(3);
    expect(within(dialog).getByText("In flight")).toBeInTheDocument();
    expect(within(dialog).getByText("Unknown")).toBeInTheDocument();
    expect(within(dialog).getByText(reason)).toBeInTheDocument();
    expectGetBackfillQuery(9);
  });

  it("requests the visible slot page and resets pagination after close", async () => {
    const backfill = makeBackfill();

    mocks.getBackfill.mockReturnValue({
      data: backfill,
      error: undefined,
      isLoading: false,
    });
    mocks.listBackfillDagRuns.mockReturnValue({
      data: { backfill_dag_runs: dagRuns, total_entries: 26 },
      error: undefined,
      isFetching: false,
      isLoading: false,
    });
    mocks.listBackfills.mockReturnValue({
      data: { backfills: [backfill], total_entries: 1 },
      error: undefined,
      isFetching: false,
      isLoading: false,
    });

    renderBackfills("/dags/example_dag/backfills/7");

    const dialog = await screen.findByRole("dialog");

    fireEvent.click(within(dialog).getByRole("button", { name: "next page" }));
    await waitFor(() =>
      expect(mocks.listBackfillDagRuns).toHaveBeenLastCalledWith(
        {
          backfillId: 7,
          limit: 25,
          offset: 25,
        },
        undefined,
        expect.objectContaining({ enabled: true }),
      ),
    );

    fireEvent.click(within(dialog).getByRole("button", { name: "Close" }));
    await waitForElementToBeRemoved(dialog);
    fireEvent.click(screen.getByRole("button", { name: "View slots for Backfill #7" }));
    await screen.findByRole("dialog");
    await waitFor(() =>
      expect(mocks.listBackfillDagRuns).toHaveBeenLastCalledWith(
        {
          backfillId: 7,
          limit: 25,
          offset: 0,
        },
        undefined,
        expect.objectContaining({ enabled: true }),
      ),
    );
  });
});
