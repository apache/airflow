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
import "@testing-library/jest-dom";
import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";

import { UseDagRunServiceGetDagRunsKeyFn } from "openapi/queries";
import { DagRunService } from "openapi/requests/services.gen";
import type { DAGRunCollectionResponse } from "openapi/requests/types.gen";

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

afterEach(() => vi.restoreAllMocks());

describe("PausedDagOptions", () => {
  it("starts collapsed and shows the choice that will be applied", () => {
    render(<PausedDagOptions dagId={DAG_ID} onChange={vi.fn()} value="keepPaused" />, {
      wrapper: createWrapper(createQueryClient()),
    });

    const trigger = screen.getByRole("button", { name: /pausedDag\.title/u });

    expect(trigger).toHaveAttribute("aria-expanded", "false");
    expect(trigger).toHaveTextContent("pausedDag.keepPaused");
  });

  it("reports the option the user picks", () => {
    const onChange = vi.fn();

    render(<PausedDagOptions dagId={DAG_ID} onChange={onChange} value="unpause" />, {
      wrapper: createWrapper(createQueryClient()),
    });

    fireEvent.click(screen.getByRole("button", { name: /pausedDag\.title/u }));
    fireEvent.click(screen.getByText("pausedDag.drain"));

    expect(onChange).toHaveBeenCalledWith("drain");
  });

  it("only looks up unfinished runs once draining is selected", async () => {
    const getDagRuns = serveUnfinishedRuns(0);
    const { rerender } = render(<PausedDagOptions dagId={DAG_ID} onChange={vi.fn()} value="unpause" />, {
      wrapper: createWrapper(createQueryClient()),
    });

    expect(getDagRuns).not.toHaveBeenCalled();

    rerender(<PausedDagOptions dagId={DAG_ID} onChange={vi.fn()} value="drain" />);

    await waitFor(() => expect(getDagRuns).toHaveBeenCalledTimes(1));
    expect(getDagRuns).toHaveBeenCalledWith(expect.objectContaining(UNFINISHED_RUNS_QUERY));
  });

  it("warns that unfinished runs will run as well when draining", async () => {
    serveUnfinishedRuns(3);

    render(<PausedDagOptions dagId={DAG_ID} onChange={vi.fn()} value="drain" />, {
      wrapper: createWrapper(createQueryClient()),
    });

    expect(await screen.findByText("pausedDag.unfinishedRunsWillRun:3")).toBeInTheDocument();
  });

  it("ignores an unfinished-run count cached before the earlier runs finished", async () => {
    const getDagRuns = serveUnfinishedRuns(0);
    const queryClient = createQueryClient();
    const queryKey = UseDagRunServiceGetDagRunsKeyFn(UNFINISHED_RUNS_QUERY);

    // Left behind by an earlier drained trigger, while its run was still queued.
    queryClient.setQueryData<DAGRunCollectionResponse>(queryKey, { dag_runs: [], total_entries: 1 });

    const { rerender } = render(<PausedDagOptions dagId={DAG_ID} onChange={vi.fn()} value="unpause" />, {
      wrapper: createWrapper(queryClient),
    });

    rerender(<PausedDagOptions dagId={DAG_ID} onChange={vi.fn()} value="drain" />);

    expect(screen.queryByText(/pausedDag\.unfinishedRunsWillRun/u)).not.toBeInTheDocument();
    await waitFor(() => expect(getDagRuns).toHaveBeenCalledTimes(1));
    await waitFor(() =>
      expect(queryClient.getQueryData<DAGRunCollectionResponse>(queryKey)?.total_entries).toBe(0),
    );
    expect(screen.queryByText(/pausedDag\.unfinishedRunsWillRun/u)).not.toBeInTheDocument();
  });
});
