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
import { act, render, screen, waitFor } from "@testing-library/react";
import { http, HttpResponse } from "msw";
import { setupServer, type SetupServer } from "msw/node";
import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it, vi } from "vitest";

import { handlers } from "src/mocks/handlers";
import { AppWrapper } from "src/utils/AppWrapper";

let server: SetupServer;

/** Requests the page has made to the bundles endpoint since the last reset. */
let requestCount = 0;

const bundleRow = (name: string, version: string | null) => ({
  active: true,
  bundle_url: null,
  import_error_count: 0,
  last_refreshed: "2026-09-10T12:00:00Z",
  name,
  team_name: null,
  version,
});

/** Serve the bundle list, counting calls, with the row content decided per request. */
const serveBundles = (rowsFor: (call: number) => Array<ReturnType<typeof bundleRow>>) => {
  server.use(
    http.get("/api/v2/dagBundles", () => {
      requestCount += 1;

      const rows = rowsFor(requestCount);

      return HttpResponse.json({ dag_bundles: rows, total_entries: rows.length });
    }),
  );
};

/** Override `[api] auto_refresh_interval`, which is in seconds. */
const serveAutoRefreshInterval = (seconds: number) => {
  server.use(
    http.get("/ui/config", () =>
      HttpResponse.json({
        auto_refresh_interval: seconds,
        default_wrap: false,
        enable_swagger_ui: true,
        hide_paused_dags_by_default: false,
        instance_name: "Airflow",
        multi_team: false,
        page_size: 15,
        require_confirmation_dag_change: false,
        test_connection: "Disabled",
      }),
    ),
  );
};

/**
 * Let real time pass while fake timers drive the refetch scheduling.
 *
 * React Query schedules refetches on `setInterval`, so the tick has to be faked to be observable
 * without waiting in real time. The awaits let the resulting fetch and re-render settle.
 */
const advance = async (ms: number) => {
  await act(async () => {
    await vi.advanceTimersByTimeAsync(ms);
  });
};

/**
 * Render the page and wait until its request count stops moving.
 *
 * `[api] auto_refresh_interval` arrives from its own query, so the list is fetched once before the
 * config lands and again once it does. Both are startup traffic rather than a refresh tick, so
 * every assertion below counts from the settled total instead of from zero.
 */
const renderSettled = async (): Promise<number> => {
  render(<AppWrapper initialEntries={["/dag_bundles"]} />);

  await screen.findByTestId("table-list");
  // One second is far below the ten-second floor, so this lets the config query and the refetch
  // it triggers finish without ever absorbing a refresh tick.
  await advance(1000);

  return requestCount;
};

beforeAll(() => {
  server = setupServer(...handlers);
  server.listen({ onUnhandledRequest: "bypass" });
});

beforeEach(() => {
  requestCount = 0;
  vi.useFakeTimers({ shouldAdvanceTime: true });
});

afterEach(() => {
  vi.useRealTimers();
  server.resetHandlers();
  localStorage.clear();
});

afterAll(() => server.close());

describe("Dag Bundles auto-refresh", () => {
  it("floors a short configured interval at ten seconds", async () => {
    // 3s is the shipped default, tuned for Grid/Graph run state. A bundle row changes at most
    // once per the bundle's refresh_interval (default 300s), so the page floors it.
    serveAutoRefreshInterval(3);
    serveBundles(() => [bundleRow("dags-folder", null)]);

    const settled = await renderSettled();

    // Still nothing new at the configured 3s, so the floor is doing the work.
    await advance(3000);
    expect(requestCount).toBe(settled);

    // Ten seconds in, it has refreshed. Counted as "more than before" rather than an exact
    // delta: StrictMode mounts the page twice, so each tick issues one request per observer.
    await advance(7000);
    expect(requestCount).toBeGreaterThan(settled);
  });

  it("honours a configured interval longer than the floor", async () => {
    serveAutoRefreshInterval(30);
    serveBundles(() => [bundleRow("dags-folder", null)]);

    const settled = await renderSettled();

    // The floor must not pull a deliberately slower cadence back down to ten seconds.
    await advance(10_000);
    expect(requestCount).toBe(settled);

    await advance(20_000);
    expect(requestCount).toBeGreaterThan(settled);
  });

  it("stops refreshing when the configured interval is zero", async () => {
    // 0 is how an operator turns auto-refresh off, so it has to short-circuit before the floor.
    serveAutoRefreshInterval(0);
    serveBundles(() => [bundleRow("dags-folder", null)]);

    const settled = await renderSettled();

    await advance(60_000);
    expect(requestCount).toBe(settled);
  });

  it("shows the new version once a refresh returns a changed row", async () => {
    // Driven by a flag rather than the request number: StrictMode issues two requests on mount, so
    // keying the change off "the second call" would flip the version before the page even settles.
    let deployedVersion = "aaaaaaa1111";

    serveAutoRefreshInterval(3);
    serveBundles(() => [bundleRow("my-git-repo", deployedVersion)]);

    await renderSettled();

    // Versions render as the seven-character prefix.
    expect(screen.getByText("aaaaaaa")).toBeInTheDocument();

    deployedVersion = "bbbbbbb2222";
    await advance(10_000);

    await waitFor(() => expect(screen.getByText("bbbbbbb")).toBeInTheDocument());
    expect(screen.queryByText("aaaaaaa")).not.toBeInTheDocument();
  });
});
