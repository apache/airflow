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
import type { PropsWithChildren } from "react";

import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { act, cleanup, renderHook, waitFor } from "@testing-library/react";
import { http, HttpResponse } from "msw";
import { afterEach, expect, it, vi } from "vitest";

import { successDag } from "src/mocks/handlers/dags";
import { server } from "src/mocks/server";

import { useDagRunStateCounts } from "./useDagRunStateCounts";
import { useDags } from "./useDags";

afterEach(() => {
  cleanup();
  vi.useRealTimers();
});

it.each(["latest run", "run counts"])("refreshes %s when no runs are pending", async (target) => {
  vi.useFakeTimers({ shouldAdvanceTime: true });
  let completed = false;
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  const wrapper = ({ children }: PropsWithChildren) => (
    <QueryClientProvider client={client}>{children}</QueryClientProvider>
  );

  server.use(
    http.get("/ui/config", () => HttpResponse.json({ auto_refresh_interval: 1 })),
    http.get("/api/v2/dags/~/dagRuns", () => HttpResponse.json({ dag_runs: [], total_entries: 0 })),
    http.get("/ui/dags", () =>
      HttpResponse.json({
        dags: [
          {
            ...successDag,
            latest_dag_runs: successDag.latest_dag_runs.map((run) => ({
              ...run,
              state: completed ? "success" : "queued",
            })),
          },
        ],
        total_entries: 1,
      }),
    ),
    http.get("/ui/dags/run_state_counts", () =>
      HttpResponse.json({
        dags: [{ dag_id: successDag.dag_id, state_counts: { success: completed ? 1 : 0 } }],
      }),
    ),
  );

  const { result } = renderHook(
    () => {
      const dags = useDags({ dagRunsLimit: 1 });
      const counts = useDagRunStateCounts({ dagIds: [successDag.dag_id], dags: dags.data?.dags });

      return { counts, dags };
    },
    { wrapper },
  );

  await waitFor(() => {
    expect(result.current.dags.data?.dags[0]?.latest_dag_runs[0]?.state).toBe("queued");
    expect(result.current.counts.data).toBeDefined();
  });
  completed = true;
  await act(async () => {
    await vi.advanceTimersByTimeAsync(12_000);
  });

  if (target === "latest run") {
    expect(result.current.dags.data?.dags[0]?.latest_dag_runs[0]?.state).toBe("success");
  } else {
    expect(result.current.counts.data).toEqual({
      dags: [{ dag_id: successDag.dag_id, state_counts: { success: 1 } }],
    });
  }
  client.clear();
});
