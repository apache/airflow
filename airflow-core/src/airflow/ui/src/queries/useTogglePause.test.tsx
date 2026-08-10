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
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { renderHook, waitFor } from "@testing-library/react";
import { http, HttpResponse } from "msw";
import { setupServer } from "msw/node";
import React from "react";
import { afterAll, afterEach, beforeAll, describe, expect, it } from "vitest";

import { UseDagServiceGetDagsUiKeyFn } from "openapi/queries";
import type { DAGWithLatestDagRunsCollectionResponse } from "openapi/requests/types.gen";
import { useTogglePause } from "src/queries/useTogglePause";

const DAG_ID = "dag_under_test";

const buildDagsList = (isPaused: boolean): DAGWithLatestDagRunsCollectionResponse => ({
  dags: [
    {
      asset_expression: null,
      bundle_name: null,
      bundle_version: null,
      dag_display_name: DAG_ID,
      dag_id: DAG_ID,
      description: null,
      file_token: "",
      fileloc: "/dags/dag.py",
      has_import_errors: false,
      has_task_concurrency_limits: false,
      is_favorite: false,
      is_paused: isPaused,
      is_stale: false,
      last_expired: null,
      last_parsed_time: null,
      latest_dag_runs: [],
      max_active_runs: 16,
      max_active_tasks: 16,
      max_consecutive_failed_dag_runs: 0,
      next_dagrun_create_after: null,
      next_dagrun_data_interval_end: null,
      next_dagrun_data_interval_start: null,
      next_dagrun_logical_date: null,
      next_dagrun_run_after: null,
      owners: ["airflow"],
      pending_actions: 0,
      tags: [],
      timetable_description: null,
      timetable_partitioned: false,
      timetable_summary: null,
    } as unknown as DAGWithLatestDagRunsCollectionResponse["dags"][number],
  ],
  total_entries: 1,
});

const server = setupServer();

const createWrapper =
  (queryClient: QueryClient) =>
  ({ children }: { readonly children: React.ReactNode }) => (
    <QueryClientProvider client={queryClient}>{children}</QueryClientProvider>
  );

const seedClient = (initialIsPaused: boolean) => {
  const queryClient = new QueryClient({
    defaultOptions: {
      mutations: { retry: false },
      queries: { gcTime: Infinity, retry: false, staleTime: Infinity },
    },
  });
  const dagsListKey = UseDagServiceGetDagsUiKeyFn({ dagRunsLimit: 1 });
  // A sibling list query with a different filter (paused=false), so tests can verify the
  // prefix-keyed optimistic write and invalidation reach every dags-list cache, not just one.
  const filteredListKey = UseDagServiceGetDagsUiKeyFn({ dagRunsLimit: 1, paused: false });

  queryClient.setQueryData<DAGWithLatestDagRunsCollectionResponse>(
    dagsListKey,
    buildDagsList(initialIsPaused),
  );
  queryClient.setQueryData<DAGWithLatestDagRunsCollectionResponse>(
    filteredListKey,
    buildDagsList(initialIsPaused),
  );

  return { dagsListKey, filteredListKey, queryClient };
};

beforeAll(() => server.listen({ onUnhandledRequest: "error" }));
afterEach(() => server.resetHandlers());
afterAll(() => server.close());

describe("useTogglePause", () => {
  it("optimistically flips is_paused in the dags list cache as soon as mutate() is called", async () => {
    server.use(
      http.patch("*/api/v2/dags/:dagId", async () => {
        // Make the server slow so the test can observe the optimistic state
        // before the response is settled.
        await new Promise((resolve) => {
          setTimeout(resolve, 50);
        });

        return HttpResponse.json({ dag_id: DAG_ID, is_paused: true });
      }),
    );

    const { dagsListKey, filteredListKey, queryClient } = seedClient(false);
    const { result } = renderHook(() => useTogglePause({ dagId: DAG_ID }), {
      wrapper: createWrapper(queryClient),
    });

    result.current.mutate({ dagId: DAG_ID, requestBody: { is_paused: true } });

    // The Switch is bound to is_paused — the optimistic update must apply before the network
    // response settles so the user sees the flip on click. It is keyed by the list prefix, so
    // every dags-list cache flips, including a sibling query with a different filter.
    await waitFor(() => {
      expect(
        queryClient.getQueryData<DAGWithLatestDagRunsCollectionResponse>(dagsListKey)?.dags[0]?.is_paused,
      ).toBe(true);
      expect(
        queryClient.getQueryData<DAGWithLatestDagRunsCollectionResponse>(filteredListKey)?.dags[0]?.is_paused,
      ).toBe(true);
    });

    await waitFor(() => expect(result.current.isPending).toBe(false));
    // Post-settle the cache still reflects the new value.
    const settled = queryClient.getQueryData<DAGWithLatestDagRunsCollectionResponse>(dagsListKey);

    expect(settled?.dags[0]?.is_paused).toBe(true);
  });

  it("rolls back the optimistic update when the server rejects the change", async () => {
    server.use(http.patch("*/api/v2/dags/:dagId", () => new HttpResponse(null, { status: 500 })));

    const { dagsListKey, queryClient } = seedClient(false);
    const { result } = renderHook(() => useTogglePause({ dagId: DAG_ID }), {
      wrapper: createWrapper(queryClient),
    });

    result.current.mutate({ dagId: DAG_ID, requestBody: { is_paused: true } });

    await waitFor(() => expect(result.current.isError).toBe(true));

    const cached = queryClient.getQueryData<DAGWithLatestDagRunsCollectionResponse>(dagsListKey);

    expect(cached?.dags[0]?.is_paused).toBe(false);
  });

  it("invalidates the dags list query on settle so filtered lists refetch", async () => {
    server.use(
      http.patch("*/api/v2/dags/:dagId", () => HttpResponse.json({ dag_id: DAG_ID, is_paused: true })),
    );

    const { dagsListKey, filteredListKey, queryClient } = seedClient(false);
    const { result } = renderHook(() => useTogglePause({ dagId: DAG_ID }), {
      wrapper: createWrapper(queryClient),
    });

    result.current.mutate({ dagId: DAG_ID, requestBody: { is_paused: true } });

    await waitFor(() => expect(result.current.isPending).toBe(false));

    // Both the seeded list and a sibling list with a different filter (paused=false) are marked
    // stale, because invalidation is keyed by the shared prefix — so a filtered consumer refetches
    // and may drop the dag from the visible page.
    expect(queryClient.getQueryState(dagsListKey)?.isInvalidated).toBe(true);
    expect(queryClient.getQueryState(filteredListKey)?.isInvalidated).toBe(true);
  });
});
