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
import type { ReactNode } from "react";

import { ChakraProvider, defaultSystem } from "@chakra-ui/react";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import "@testing-library/jest-dom/vitest";
import { act, fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { UseBackfillServiceListBackfillsUiKeyFn, UseDagRunServiceGetDagRunsKeyFn } from "openapi/queries";
import { CancelablePromise } from "openapi/requests/core/CancelablePromise";
import { BackfillService, DagRunService } from "openapi/requests/services.gen";
import type {
  BackfillCollectionResponse,
  BackfillResponse,
  DAGRunCollectionResponse,
} from "openapi/requests/types.gen";

import PausedDagOptions from "./PausedDagOptions";

vi.mock("react-i18next", () => ({
  useTranslation: () => ({
    // eslint-disable-next-line id-length
    t: (key: string, opts?: { count?: number }) =>
      opts?.count === undefined ? key : `${key}:${String(opts.count)}`,
  }),
}));

const DAG_ID = "paused_dag";
const UNFINISHED_RUNS_QUERY = { dagId: DAG_ID, limit: 1, state: ["queued", "running"] };

const serveUnfinishedRuns = (totalEntries: number) =>
  vi.spyOn(DagRunService, "getDagRuns").mockResolvedValue({ dag_runs: [], total_entries: totalEntries });

const serveActiveBackfills = (backfills: Array<Partial<BackfillResponse>>) =>
  vi.spyOn(BackfillService, "listBackfillsUi").mockResolvedValue({
    backfills: backfills as Array<BackfillResponse>,
    total_entries: backfills.length,
  });

const createWrapper = (queryClient: QueryClient) => {
  const TestWrapper = ({ children }: { readonly children: ReactNode }) => (
    <ChakraProvider value={defaultSystem}>
      <QueryClientProvider client={queryClient}>{children}</QueryClientProvider>
    </ChakraProvider>
  );

  return TestWrapper;
};

// Mirrors the app's client: cached data stays fresh for minutes, so it is not refetched on mount.
const createQueryClient = () =>
  new QueryClient({ defaultOptions: { queries: { retry: false, staleTime: 5 * 60 * 1000 } } });

beforeEach(() => {
  serveActiveBackfills([]);
});

afterEach(() => vi.restoreAllMocks());

const COUNT_TEXT = /pausedDag\.unfinishedRunsWillRun/u;

const getOptionCard = (label: string) => {
  const card = screen.getByText(label).closest("label");

  if (card === null) {
    throw new Error(`No option card for ${label}`);
  }

  return card;
};

describe("PausedDagOptions", () => {
  it("shows every option without expanding a section", () => {
    render(<PausedDagOptions dagId={DAG_ID} onChange={vi.fn()} value="unpause" />, {
      wrapper: createWrapper(createQueryClient()),
    });

    expect(screen.getByText("pausedDag.unpause")).toBeVisible();
    expect(screen.getByText("pausedDag.drain")).toBeVisible();
    expect(screen.getByText("pausedDag.keepPaused")).toBeVisible();
    expect(screen.getByRole("radiogroup", { name: "pausedDag.title" })).toBeVisible();
  });

  it("reports the option the user picks", async () => {
    const onChange = vi.fn();

    render(<PausedDagOptions dagId={DAG_ID} onChange={onChange} value="unpause" />, {
      wrapper: createWrapper(createQueryClient()),
    });

    fireEvent.click(screen.getByText("pausedDag.drain"));

    await waitFor(() => expect(onChange).toHaveBeenCalledWith("drain"));
  });

  it("shows the unfinished-run count on the options that let those runs proceed", async () => {
    const getDagRuns = serveUnfinishedRuns(3);

    render(<PausedDagOptions dagId={DAG_ID} onChange={vi.fn()} value="unpause" />, {
      wrapper: createWrapper(createQueryClient()),
    });

    await waitFor(() =>
      expect(getDagRuns).toHaveBeenCalledWith(expect.objectContaining(UNFINISHED_RUNS_QUERY)),
    );
    expect(
      await within(getOptionCard("pausedDag.unpause")).findByText("pausedDag.unfinishedRunsWillRun:3"),
    ).toBeInTheDocument();
    expect(
      within(getOptionCard("pausedDag.drain")).getByText("pausedDag.unfinishedRunsWillRun:3"),
    ).toBeInTheDocument();
    expect(within(getOptionCard("pausedDag.keepPaused")).queryByText(COUNT_TEXT)).not.toBeInTheDocument();
  });

  it("shows no count when the Dag has no unfinished runs", async () => {
    const getDagRuns = serveUnfinishedRuns(0);

    render(<PausedDagOptions dagId={DAG_ID} onChange={vi.fn()} value="drain" />, {
      wrapper: createWrapper(createQueryClient()),
    });

    await waitFor(() => expect(getDagRuns).toHaveBeenCalledTimes(1));
    expect(screen.queryByText(COUNT_TEXT)).not.toBeInTheDocument();
  });

  it.each([
    { expectedRunType: undefined, isPaused: false },
    {
      expectedRunType: [
        "scheduled",
        "manual",
        "operator_triggered",
        "asset_triggered",
        "asset_materialization",
      ],
      isPaused: true,
    },
  ])(
    "leaves out backfill runs only while the backfill is paused (paused=$isPaused)",
    async ({ expectedRunType, isPaused }) => {
      serveActiveBackfills([{ dag_id: DAG_ID, id: 1, is_paused: isPaused }]);
      const getDagRuns = serveUnfinishedRuns(2);

      render(<PausedDagOptions dagId={DAG_ID} onChange={vi.fn()} value="drain" />, {
        wrapper: createWrapper(createQueryClient()),
      });

      await waitFor(() =>
        expect(getDagRuns).toHaveBeenLastCalledWith(
          expect.objectContaining({ ...UNFINISHED_RUNS_QUERY, runType: expectedRunType }),
        ),
      );
      expect(await screen.findAllByText("pausedDag.unfinishedRunsWillRun:2")).toHaveLength(2);
    },
  );

  it("ignores an unfinished-run count cached before the earlier runs finished", async () => {
    const getDagRuns = serveUnfinishedRuns(0);
    const queryClient = createQueryClient();
    const queryKey = UseDagRunServiceGetDagRunsKeyFn(UNFINISHED_RUNS_QUERY);

    // Left behind by an earlier drained trigger, while its run was still queued.
    queryClient.setQueryData<DAGRunCollectionResponse>(queryKey, { dag_runs: [], total_entries: 1 });

    render(<PausedDagOptions dagId={DAG_ID} onChange={vi.fn()} value="drain" />, {
      wrapper: createWrapper(queryClient),
    });

    expect(screen.queryByText(COUNT_TEXT)).not.toBeInTheDocument();
    await waitFor(() => expect(getDagRuns).toHaveBeenCalledTimes(1));
    await waitFor(() =>
      expect(queryClient.getQueryData<DAGRunCollectionResponse>(queryKey)?.total_entries).toBe(0),
    );
    expect(screen.queryByText(COUNT_TEXT)).not.toBeInTheDocument();
  });

  it.each([false, true])(
    "waits for fresh backfill state before counting runs (cached=%s)",
    async (cached) => {
      let resolveBackfills: ((value: BackfillCollectionResponse) => void) | undefined;

      vi.mocked(BackfillService.listBackfillsUi).mockReturnValue(
        new CancelablePromise((resolve) => {
          resolveBackfills = resolve;
        }),
      );
      const getDagRuns = serveUnfinishedRuns(2);
      const queryClient = createQueryClient();

      if (cached) {
        queryClient.setQueryData(UseBackfillServiceListBackfillsUiKeyFn({ active: true, dagId: DAG_ID }), {
          backfills: [{ dag_id: DAG_ID, id: 1, is_paused: false }],
          total_entries: 1,
        });
      }
      render(<PausedDagOptions dagId={DAG_ID} onChange={vi.fn()} value="drain" />, {
        wrapper: createWrapper(queryClient),
      });
      await waitFor(() => expect(BackfillService.listBackfillsUi).toHaveBeenCalled());
      expect(getDagRuns).not.toHaveBeenCalled();
      expect(screen.queryByText(COUNT_TEXT)).not.toBeInTheDocument();
      act(() => {
        resolveBackfills?.({
          backfills: [{ dag_id: DAG_ID, id: 1, is_paused: true } as BackfillResponse],
          total_entries: 1,
        });
      });
      expect(await screen.findAllByText("pausedDag.unfinishedRunsWillRun:2")).toHaveLength(2);
      expect(getDagRuns).toHaveBeenCalledTimes(1);
      expect(getDagRuns).toHaveBeenCalledWith(
        expect.objectContaining({
          runType: ["scheduled", "manual", "operator_triggered", "asset_triggered", "asset_materialization"],
        }),
      );
    },
  );

  it("does not count runs when the backfill refresh fails", async () => {
    vi.mocked(BackfillService.listBackfillsUi).mockRejectedValue(new Error("Unavailable"));
    const getDagRuns = serveUnfinishedRuns(2);
    const queryClient = createQueryClient();
    const queryKey = UseBackfillServiceListBackfillsUiKeyFn({ active: true, dagId: DAG_ID });

    queryClient.setQueryData(queryKey, { backfills: [], total_entries: 0 });
    render(<PausedDagOptions dagId={DAG_ID} onChange={vi.fn()} value="drain" />, {
      wrapper: createWrapper(queryClient),
    });
    await waitFor(() => expect(queryClient.getQueryState(queryKey)?.status).toBe("error"));
    expect(getDagRuns).not.toHaveBeenCalled();
    expect(screen.queryByText(COUNT_TEXT)).not.toBeInTheDocument();
  });
});
