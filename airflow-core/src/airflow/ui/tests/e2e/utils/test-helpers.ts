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

/**
 * Shared E2E test utilities for data isolation and API-based setup.
 *
 * These helpers enforce two key principles:
 * 1. Data isolation — unique IDs per worker to prevent cross-worker interference
 * 2. API-first setup — REST API calls instead of UI interactions in beforeAll
 *
 * Template: dag-calendar-tab.spec.ts (409 handling, DAG readiness polling)
 */
import { expect, type APIRequestContext, type Locator, type Page } from "@playwright/test";

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

type RequestLike = APIRequestContext | Page;

type DagRunData = {
  conf?: Record<string, unknown>;
  dag_run_id: string;
  logical_date: string;
  note?: string;
};

type DagRunResponse = {
  dag_run_id: string;
  state: string;
};

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

/**
 * Get the APIRequestContext from either a Page or an APIRequestContext.
 * In beforeAll hooks we often have a Page (from browser.newContext().newPage()),
 * while in some cases we may receive an APIRequestContext directly.
 */
function getRequestContext(source: RequestLike): APIRequestContext {
  if ("request" in source) {
    return source.request;
  }

  return source;
}

function getBaseUrl(): string {
  return process.env.AIRFLOW_UI_BASE_URL ?? "http://localhost:28080";
}

// ---------------------------------------------------------------------------
// Data Isolation
// ---------------------------------------------------------------------------

/**
 * Generate a unique run ID with worker disambiguation.
 *
 * Format: `{prefix}_{timestamp}_{workerIndex}_{counter}`
 *
 * This prevents cross-worker collisions when fullyParallel=true and
 * multiple browser projects run simultaneously against the same Airflow instance.
 * The counter prevents collisions when multiple calls happen in the same millisecond.
 */
let idCounter = 0;

export function uniqueRunId(prefix: string): string {
  const workerIndex = process.env.TEST_WORKER_INDEX ?? "0";

  return `${prefix}_${Date.now()}_w${workerIndex}_${idCounter++}`;
}

// ---------------------------------------------------------------------------
// DAG Readiness
// ---------------------------------------------------------------------------

/**
 * Wait for a DAG to be parsed and available via the API.
 *
 * Polls GET /api/v2/dags/{dagId} until it returns 200.
 * Extracted from dag-runs.spec.ts and dag-calendar-tab.spec.ts.
 */
export async function waitForDagReady(
  source: RequestLike,
  dagId: string,
  options?: { timeout?: number },
): Promise<void> {
  const request = getRequestContext(source);
  const baseUrl = getBaseUrl();
  const timeout = options?.timeout ?? 60_000;

  await expect
    .poll(
      async () => {
        const response = await request.get(`${baseUrl}/api/v2/dags/${dagId}`, { timeout: 30_000 });

        return response.ok();
      },
      { intervals: [2000], timeout },
    )
    .toBe(true);
}

// ---------------------------------------------------------------------------
// DAG Run API Helpers
// ---------------------------------------------------------------------------

/**
 * Trigger a DAG run via the API with an auto-generated run ID.
 *
 * Simpler wrapper around POST /api/v2/dags/{dagId}/dagRuns that
 * auto-generates a unique dag_run_id. Returns the dagRunId and logicalDate
 * so callers can poll a specific run rather than the latest run.
 *
 * On 409 (dag_run_id conflict), returns the generated ID — this should not
 * normally happen because uniqueRunId() includes a timestamp+worker+counter.
 */
export async function apiTriggerDagRun(
  source: RequestLike,
  dagId: string,
  options?: { runId?: string },
): Promise<{ dagRunId: string; logicalDate: string }> {
  const request = getRequestContext(source);
  const baseUrl = getBaseUrl();
  const dagRunId = options?.runId ?? uniqueRunId(dagId);
  const logicalDate = new Date().toISOString();

  const response = await request.post(`${baseUrl}/api/v2/dags/${dagId}/dagRuns`, {
    data: {
      dag_run_id: dagRunId,
      logical_date: logicalDate,
      note: "e2e test",
    },
    headers: { "Content-Type": "application/json" },
  });

  // 409 = run already exists; return the ID we tried to create.
  if (response.status() === 409) {
    return { dagRunId, logicalDate };
  }

  if (!response.ok()) {
    const body = await response.text();

    throw new Error(`DAG run trigger failed (${response.status()}): ${body}`);
  }

  const json = (await response.json()) as { logical_date?: string } & DagRunResponse;

  return {
    dagRunId: json.dag_run_id,
    logicalDate: (json as { logical_date?: string }).logical_date ?? logicalDate,
  };
}

/**
 * Create a DAG run via the API with 409 conflict handling.
 *
 * On 409 (run already exists at this logical_date — parallel worker race),
 * returns the existing run ID rather than throwing.
 *
 * Template: dag-calendar-tab.spec.ts:59-79
 */
export async function apiCreateDagRun(source: RequestLike, dagId: string, data: DagRunData): Promise<string> {
  const request = getRequestContext(source);
  const baseUrl = getBaseUrl();

  const response = await request.post(`${baseUrl}/api/v2/dags/${dagId}/dagRuns`, {
    data: {
      conf: data.conf ?? {},
      dag_run_id: data.dag_run_id,
      logical_date: data.logical_date,
      note: data.note ?? "e2e test",
    },
    headers: { "Content-Type": "application/json" },
  });

  // 409 = a run at this logical_date already exists (parallel worker race);
  // another worker's beforeAll already created the test data.
  if (response.status() === 409) {
    return data.dag_run_id;
  }

  if (!response.ok()) {
    const body = await response.text();

    throw new Error(`DAG run creation failed (${response.status()}): ${body}`);
  }

  const json = (await response.json()) as DagRunResponse;

  return json.dag_run_id;
}

/**
 * Set a DAG run's state via the API.
 *
 * Handles 409 conflicts gracefully.
 */
export async function apiSetDagRunState(
  source: RequestLike,
  options: { dagId: string; runId: string; state: "failed" | "queued" | "success" },
): Promise<void> {
  const { dagId, runId, state } = options;
  const request = getRequestContext(source);
  const baseUrl = getBaseUrl();

  const response = await request.patch(`${baseUrl}/api/v2/dags/${dagId}/dagRuns/${runId}`, {
    data: { state },
    headers: { "Content-Type": "application/json" },
  });

  // 409 = state already changed by another worker or scheduler; acceptable.
  if (response.status() === 409) {
    return;
  }

  if (!response.ok()) {
    const body = await response.text();

    throw new Error(`Set DAG run state failed (${response.status()}): ${body}`);
  }
}

/**
 * Wait for a DAG run to reach a specific state by polling the API.
 *
 * Replaces UI-based status polling (DagsPage.verifyDagRunStatus) which
 * uses page reloads and 7-minute timeouts.
 */
export async function waitForDagRunStatus(
  source: RequestLike,
  options: { dagId: string; expectedState: string; runId: string; timeout?: number },
): Promise<void> {
  const { dagId, expectedState, runId } = options;
  const request = getRequestContext(source);
  const baseUrl = getBaseUrl();
  const timeout = options.timeout ?? 120_000;

  await expect
    .poll(
      async () => {
        const response = await request.get(`${baseUrl}/api/v2/dags/${dagId}/dagRuns/${runId}`, {
          timeout: 30_000,
        });

        if (!response.ok()) {
          return "unknown";
        }

        const data = (await response.json()) as DagRunResponse;

        if (data.state === "failed" && expectedState !== "failed") {
          throw new Error(`DAG run ${runId} failed unexpectedly`);
        }

        return data.state;
      },
      {
        intervals: [5000],
        message: `DAG run ${runId} did not reach state "${expectedState}" within ${timeout}ms`,
        timeout,
      },
    )
    .toBe(expectedState);
}

// ---------------------------------------------------------------------------
// Variable API Helpers
// ---------------------------------------------------------------------------

/**
 * Create a variable via the API with 409 conflict handling.
 */
export async function apiCreateVariable(
  source: RequestLike,
  options: { description?: string; key: string; value: string },
): Promise<void> {
  const { description, key, value } = options;
  const request = getRequestContext(source);
  const baseUrl = getBaseUrl();

  const response = await request.post(`${baseUrl}/api/v2/variables`, {
    data: { description: description ?? "", key, value },
    headers: { "Content-Type": "application/json" },
  });

  // 409 = variable already exists (parallel worker or leftover from prior run).
  if (response.status() === 409) {
    return;
  }

  if (!response.ok()) {
    const body = await response.text();

    throw new Error(`Variable creation failed (${response.status()}): ${body}`);
  }
}

/**
 * Delete a variable via the API. Handles 404 (already deleted).
 */
export async function apiDeleteVariable(source: RequestLike, key: string): Promise<void> {
  const request = getRequestContext(source);
  const baseUrl = getBaseUrl();

  const response = await request.delete(`${baseUrl}/api/v2/variables/${encodeURIComponent(key)}`);

  // 404 = already deleted by another worker or cleanup; acceptable.
  if (response.status() === 404) {
    return;
  }

  if (!response.ok()) {
    const body = await response.text();

    throw new Error(`Variable deletion failed (${response.status()}): ${body}`);
  }
}

// ---------------------------------------------------------------------------
// UI Wait Utilities
// ---------------------------------------------------------------------------

/**
 * Wait for a table to load by checking that at least one row is visible.
 *
 * Replaces `document.querySelector('[data-testid="table-list"]')` via
 * page.evaluate patterns found in EventsPage, ProvidersPage, VariablePage, etc.
 */
export async function waitForTableLoad(
  page: Page,
  options?: { checkSkeletons?: boolean; testId?: string; timeout?: number },
): Promise<void> {
  const testId = options?.testId ?? "table-list";
  const timeout = options?.timeout ?? 30_000;

  const table = page.getByTestId(testId);

  await expect(table).toBeVisible({ timeout });

  // Optionally wait for skeleton placeholders to disappear.
  // Only check when explicitly requested, since some pages (e.g., XComs)
  // use [data-scope="skeleton"] for lazy-loaded cell content, not table loading.
  if (options?.checkSkeletons) {
    await expect(table.locator('[data-testid="skeleton"], [data-scope="skeleton"]')).toHaveCount(0, {
      timeout,
    });
  }

  const firstRow = table.locator("tbody tr").first();
  const emptyMessage = page.getByText(/no .* found/i);

  await expect(firstRow.or(emptyMessage)).toBeVisible({ timeout });
}

/**
 * Wait for DOM row count to stabilize (stop changing between measurements).
 *
 * Extracted from ConnectionsPage.ts pattern (lines 307-331, 492-509).
 * Uses expect.toPass() with intervals to detect stability — each retry
 * naturally provides the delay between measurements without explicit sleeps.
 *
 * How it works: on each attempt, we read the count and compare it to the
 * previous attempt's count. The interval between retries acts as the natural
 * delay. If count hasn't changed between two consecutive attempts, we consider
 * the DOM stable.
 */
export async function waitForStableRowCount(
  _page: Page,
  rowLocator: Locator,
  options?: { timeout?: number },
): Promise<number> {
  const timeout = options?.timeout ?? 10_000;
  let previousCount = -1;
  let stableCount = 0;

  await expect(async () => {
    const currentCount = await rowLocator.count();

    // Save current for comparison on next retry, BEFORE the assertion.
    const prev = previousCount;

    previousCount = currentCount;

    // Must have rows and must match previous measurement.
    // First attempt always fails (prev is -1), which is intentional —
    // we need at least two measurements to confirm stability.
    expect(currentCount).toBeGreaterThan(0);
    expect(currentCount).toBe(prev);

    stableCount = currentCount;
  }).toPass({
    intervals: [200, 300, 500],
    timeout,
  });

  return stableCount;
}
