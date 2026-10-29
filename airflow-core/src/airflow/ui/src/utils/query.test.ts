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
import { renderHook, waitFor } from "@testing-library/react";
import { http, HttpResponse } from "msw";
import { expect, it } from "vitest";

import { server } from "src/mocks/server";
import { Wrapper } from "src/utils/Wrapper";
import { useAutoRefresh } from "src/utils/query";

it("refreshes when pending runs exist without fetching any of them", async () => {
  const autoRefreshInterval = 5;
  const pendingRuns = [{ dag_run_id: "running_run" }, { dag_run_id: "queued_run" }];
  const requestedLimits: Array<string | null> = [];

  server.use(
    http.get("/ui/config", () => HttpResponse.json({ auto_refresh_interval: autoRefreshInterval })),
    http.get("/api/v2/dags/~/dagRuns", ({ request }) => {
      const limit = new URL(request.url).searchParams.get("limit");

      requestedLimits.push(limit);

      return HttpResponse.json({
        dag_runs: pendingRuns.slice(0, Number(limit)),
        total_entries: pendingRuns.length,
      });
    }),
  );

  const { result } = renderHook(() => useAutoRefresh({ checkPendingRuns: true }), { wrapper: Wrapper });

  await waitFor(() => expect(result.current).toBe(autoRefreshInterval * 1000));
  expect(new Set(requestedLimits)).toEqual(new Set(["0"]));
});
