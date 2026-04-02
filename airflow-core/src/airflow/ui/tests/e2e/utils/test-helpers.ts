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
 */
import { expect, type APIRequestContext, type Locator, type Page } from "@playwright/test";
import { randomUUID } from "node:crypto";
import { testConfig } from "playwright.config";

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

function getRequestContext(source: RequestLike): APIRequestContext {
  if ("request" in source) {
    return source.request;
  }

  return source;
}

const { baseUrl } = testConfig.connection;

/** Generate a unique run ID: `{prefix}_{uuid8}`. */
export function uniqueRunId(prefix: string): string {
  return `${prefix}_${randomUUID().slice(0, 8)}`;
}

/**
 * Poll GET /api/v2/dags/{dagId} until 200 — waits for DAG to be parsed.
 */
export async function waitForDagReady(
  source: RequestLike,
  dagId: string,
  options?: { timeout?: number },
): Promise<void> {
  const request = getRequestContext(source);

  const timeout = options?.timeout ?? 120_000;

  await expect
    .poll(
      async () => {
        try {
          const response = await request.get(`${baseUrl}/api/v2/dags/${dagId}`, { timeout: 10_000 });

          return response.ok();
        } catch {
          return false;
        }
      },
      { intervals: [2000], timeout },
    )
    .toBe(true);
}

/**
 * Trigger a DAG run with an auto-generated unique run ID.
 * Returns the dagRunId and logicalDate for targeted polling.
 */
export async function apiTriggerDagRun(
  source: RequestLike,
  dagId: string,
  options?: { runId?: string },
): Promise<{ dagRunId: string; logicalDate: string }> {
  const request = getRequestContext(source);

  let dagRunId = options?.runId ?? uniqueRunId(dagId);
  let resultLogicalDate = new Date().toISOString();

  await expect(async () => {
    // Generate a fresh logicalDate on each attempt so that a 409
    // (logical_date collision from a parallel worker) is recoverable.
    const logicalDate = new Date().toISOString();

    const response = await request.post(`${baseUrl}/api/v2/dags/${dagId}/dagRuns`, {
      data: {
        dag_run_id: dagRunId,
        logical_date: logicalDate,
        note: "e2e test",
      },
      headers: { "Content-Type": "application/json" },
      timeout: 10_000,
    });

    if (!response.ok()) {
      // On 409, regenerate dag_run_id (unless caller pinned it) so the
      // next attempt doesn't collide on either dag_run_id or logical_date.
      if (response.status() === 409 && options?.runId === undefined) {
        dagRunId = uniqueRunId(dagId);
      }

      throw new Error(`DAG run trigger failed (${response.status()})`);
    }

    const json = (await response.json()) as { logical_date?: string } & DagRunResponse;

    resultLogicalDate = json.logical_date ?? logicalDate;
  }).toPass({ intervals: [2000, 3000, 5000], timeout: 60_000 });

  return { dagRunId, logicalDate: resultLogicalDate };
}

/**
 * Create a DAG run via the API.
 */
export async function apiCreateDagRun(source: RequestLike, dagId: string, data: DagRunData): Promise<string> {
  const request = getRequestContext(source);
  let resultRunId = data.dag_run_id;

  // Track fallback values that are regenerated on 409 collisions,
  // without mutating the caller's `data` parameter.
  let retryRunId: string | undefined;
  let retryLogicalDate: string | undefined;

  await expect(async () => {
    const runId = retryRunId ?? data.dag_run_id;
    const logicalDate = retryLogicalDate ?? data.logical_date;

    const response = await request.post(`${baseUrl}/api/v2/dags/${dagId}/dagRuns`, {
      data: {
        conf: data.conf ?? {},
        dag_run_id: runId,
        logical_date: logicalDate,
        note: data.note ?? "e2e test",
      },
      headers: { "Content-Type": "application/json" },
      timeout: 10_000,
    });

    if (!response.ok()) {
      // On 409, generate fresh dag_run_id and logical_date for the next retry.
      if (response.status() === 409) {
        retryRunId = uniqueRunId(dagId);
        retryLogicalDate = new Date().toISOString();
      }

      throw new Error(`DAG run creation failed (${response.status()})`);
    }

    const json = (await response.json()) as DagRunResponse;

    resultRunId = json.dag_run_id;
  }).toPass({ intervals: [2000, 3000, 5000], timeout: 60_000 });

  return resultRunId;
}

/**
 * Set a DAG run's state via the API.
 */
export async function apiSetDagRunState(
  source: RequestLike,
  options: { dagId: string; runId: string; state: "failed" | "queued" | "success" },
): Promise<void> {
  const { dagId, runId, state } = options;
  const request = getRequestContext(source);

  await expect(async () => {
    const response = await request.patch(`${baseUrl}/api/v2/dags/${dagId}/dagRuns/${runId}`, {
      data: { state },
      headers: { "Content-Type": "application/json" },
      timeout: 10_000,
    });

    if (response.status() !== 409 && !response.ok()) {
      throw new Error(`Set DAG run state failed (${response.status()})`);
    }
  }).toPass({ intervals: [2000, 3000, 5000], timeout: 60_000 });
}

/**
 * Poll the API until the DAG run reaches the expected state.
 */
export async function waitForDagRunStatus(
  source: RequestLike,
  options: { dagId: string; expectedState: string; runId: string; timeout?: number },
): Promise<void> {
  const { dagId, expectedState, runId } = options;
  const request = getRequestContext(source);

  const timeout = options.timeout ?? 120_000;

  await expect
    .poll(
      async () => {
        try {
          const response = await request.get(`${baseUrl}/api/v2/dags/${dagId}/dagRuns/${runId}`, {
            timeout: 10_000,
          });

          if (!response.ok()) {
            return `unknown (HTTP ${response.status()})`;
          }

          const data = (await response.json()) as DagRunResponse;

          if (data.state === "failed" && expectedState !== "failed") {
            throw new Error(`DAG run ${runId} failed unexpectedly`);
          }

          return data.state;
        } catch (error) {
          // Re-throw intentional failures (unexpected "failed" state).
          if (error instanceof Error && error.message.includes("failed unexpectedly")) {
            throw error;
          }

          // Transient network/timeout errors — retry on next interval.
          return `unknown (${error instanceof Error ? error.message : "network error"})`;
        }
      },
      {
        intervals: [5000],
        message: `DAG run ${runId} did not reach state "${expectedState}" within ${timeout}ms`,
        timeout,
      },
    )
    .toBe(expectedState);
}

/**
 * Poll the API until a task instance reaches the expected state.
 * Unlike waitForDagRunStatus, this targets a specific task within a DAG run.
 */
export async function waitForTaskInstanceState(
  source: RequestLike,
  options: {
    dagId: string;
    expectedState: string;
    runId: string;
    taskId: string;
    timeout?: number;
  },
): Promise<void> {
  const { dagId, expectedState, runId, taskId } = options;
  const request = getRequestContext(source);

  const timeout = options.timeout ?? 120_000;
  const terminalStates = new Set(["success", "failed", "skipped", "removed", "upstream_failed"]);

  await expect
    .poll(
      async () => {
        try {
          const response = await request.get(
            `${baseUrl}/api/v2/dags/${dagId}/dagRuns/${runId}/taskInstances/${taskId}`,
            { timeout: 10_000 },
          );

          if (!response.ok()) {
            return "unknown";
          }

          const data = (await response.json()) as { state: string };
          const { state } = data;
          const expected = expectedState.toLowerCase();

          if (state !== expected && terminalStates.has(state)) {
            throw new Error(`Task ${taskId} reached terminal state "${state}" instead of "${expected}"`);
          }

          return state;
        } catch (error) {
          if (error instanceof Error && error.message.includes("terminal state")) {
            throw error;
          }

          return "unknown";
        }
      },
      {
        intervals: [3000, 5000],
        message: `Task ${taskId} did not reach state "${expectedState}" within ${timeout}ms`,
        timeout,
      },
    )
    .toBe(expectedState.toLowerCase());
}

/**
 * Respond to a HITL (Human-in-the-Loop) task via the API.
 * 409 is treated as success (already responded).
 */
export async function apiRespondToHITL(
  source: RequestLike,
  options: {
    chosenOptions: Array<string>;
    dagId: string;
    mapIndex?: number;
    paramsInput?: Record<string, unknown>;
    runId: string;
    taskId: string;
  },
): Promise<void> {
  const { chosenOptions, dagId, runId, taskId } = options;
  const mapIndex = options.mapIndex ?? -1;
  const paramsInput = options.paramsInput ?? {};
  const request = getRequestContext(source);

  await expect(async () => {
    const response = await request.patch(
      `${baseUrl}/api/v2/dags/${dagId}/dagRuns/${runId}/taskInstances/${taskId}/${mapIndex}/hitlDetails`,
      {
        data: { chosen_options: chosenOptions, params_input: paramsInput },
        headers: { "Content-Type": "application/json" },
        timeout: 10_000,
      },
    );

    // 409 = already responded; acceptable.
    if (response.status() !== 409 && !response.ok()) {
      throw new Error(`HITL response failed (${response.status()})`);
    }
  }).toPass({ intervals: [2000, 3000, 5000], timeout: 60_000 });
}

/**
 * Run the full HITL flow entirely via API — no browser needed.
 *
 * The example_hitl_operator DAG has 4 parallel HITL tasks, then an approval
 * task, then a branch task. This function triggers the DAG, responds to each
 * task via the API, and waits for the DAG run to complete.
 */
export async function setupHITLFlowViaAPI(
  source: RequestLike,
  dagId: string,
  approve: boolean,
): Promise<string> {
  const request = getRequestContext(source);

  await waitForDagReady(request, dagId);
  await request.patch(`${baseUrl}/api/v2/dags/${dagId}`, { data: { is_paused: false } });

  const { dagRunId } = await apiTriggerDagRun(request, dagId);

  // wait_for_default_option auto-resolves (1s timeout, defaults=["option 7"]).
  await waitForTaskInstanceState(request, {
    dagId,
    expectedState: "success",
    runId: dagRunId,
    taskId: "wait_for_default_option",
  });

  await waitForTaskInstanceState(request, {
    dagId,
    expectedState: "deferred",
    runId: dagRunId,
    taskId: "wait_for_input",
  });
  await apiRespondToHITL(request, {
    chosenOptions: ["OK"],
    dagId,
    paramsInput: { information: "Approved by test" },
    runId: dagRunId,
    taskId: "wait_for_input",
  });

  await waitForTaskInstanceState(request, {
    dagId,
    expectedState: "deferred",
    runId: dagRunId,
    taskId: "wait_for_option",
  });
  await apiRespondToHITL(request, {
    chosenOptions: ["option 1"],
    dagId,
    runId: dagRunId,
    taskId: "wait_for_option",
  });

  await waitForTaskInstanceState(request, {
    dagId,
    expectedState: "deferred",
    runId: dagRunId,
    taskId: "wait_for_multiple_options",
  });
  await apiRespondToHITL(request, {
    chosenOptions: ["option 4", "option 5"],
    dagId,
    runId: dagRunId,
    taskId: "wait_for_multiple_options",
  });

  // Wait for all parallel tasks to succeed before the approval task starts.
  await waitForTaskInstanceState(request, {
    dagId,
    expectedState: "success",
    runId: dagRunId,
    taskId: "wait_for_input",
  });
  await waitForTaskInstanceState(request, {
    dagId,
    expectedState: "success",
    runId: dagRunId,
    taskId: "wait_for_option",
  });
  await waitForTaskInstanceState(request, {
    dagId,
    expectedState: "success",
    runId: dagRunId,
    taskId: "wait_for_multiple_options",
  });

  await waitForTaskInstanceState(request, {
    dagId,
    expectedState: "deferred",
    runId: dagRunId,
    taskId: "valid_input_and_options",
  });
  await apiRespondToHITL(request, {
    chosenOptions: [approve ? "Approve" : "Reject"],
    dagId,
    runId: dagRunId,
    taskId: "valid_input_and_options",
  });
  await waitForTaskInstanceState(request, {
    dagId,
    expectedState: "success",
    runId: dagRunId,
    taskId: "valid_input_and_options",
  });

  if (approve) {
    await waitForTaskInstanceState(request, {
      dagId,
      expectedState: "deferred",
      runId: dagRunId,
      taskId: "choose_a_branch_to_run",
    });
    await apiRespondToHITL(request, {
      chosenOptions: ["task_1"],
      dagId,
      runId: dagRunId,
      taskId: "choose_a_branch_to_run",
    });
  }

  await waitForDagRunStatus(request, {
    dagId,
    expectedState: "success",
    runId: dagRunId,
    timeout: 120_000,
  });

  return dagRunId;
}

/** Delete a DAG run via the API. 404 is treated as success. */
export async function apiDeleteDagRun(source: RequestLike, dagId: string, runId: string): Promise<void> {
  const request = getRequestContext(source);

  const response = await request.delete(`${baseUrl}/api/v2/dags/${dagId}/dagRuns/${runId}`, {
    timeout: 10_000,
  });

  // 404 = already deleted by another worker or cleanup; acceptable.
  if (response.status() === 404) {
    return;
  }

  if (!response.ok()) {
    const body = await response.text();

    throw new Error(`DAG run deletion failed (${response.status()}): ${body}`);
  }
}

/**
 * Delete a DAG run, logging (not throwing) unexpected errors.
 * Use this in fixture teardown where cleanup must not abort the loop.
 * 404 is already handled inside `apiDeleteDagRun`.
 *
 * Strategy: force-fail the run first so the server doesn't wait for
 * running tasks during deletion, then delete with one retry on timeout.
 */
export async function safeCleanupDagRun(source: RequestLike, dagId: string, runId: string): Promise<void> {
  const request = getRequestContext(source);

  try {
    await request.patch(`${baseUrl}/api/v2/dags/${dagId}/dagRuns/${runId}`, {
      data: { state: "failed" },
      headers: { "Content-Type": "application/json" },
      timeout: 10_000,
    });
  } catch {
    // Run may already be terminal or deleted — ignore.
  }

  for (let attempt = 0; attempt < 2; attempt++) {
    try {
      await apiDeleteDagRun(source, dagId, runId);

      return;
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      const isTimeout = message.includes("Timeout");

      if (isTimeout && attempt === 0) {
        continue;
      }

      console.warn(`[e2e cleanup] Failed to delete DAG run ${dagId}/${runId}: ${message}`);

      return;
    }
  }
}

/** Create a variable via the API. 409 is treated as success. */
export async function apiCreateVariable(
  source: RequestLike,
  options: { description?: string; key: string; value: string },
): Promise<void> {
  const { description, key, value } = options;
  const request = getRequestContext(source);

  await expect(async () => {
    const response = await request.post(`${baseUrl}/api/v2/variables`, {
      data: { description: description ?? "", key, value },
      headers: { "Content-Type": "application/json" },
      timeout: 10_000,
    });

    if (response.status() !== 409 && !response.ok()) {
      throw new Error(`Variable creation failed (${response.status()})`);
    }
  }).toPass({ intervals: [2000, 3000, 5000], timeout: 90_000 });
}

/** Delete a variable via the API. 404 is treated as success. */
export async function apiDeleteVariable(source: RequestLike, key: string): Promise<void> {
  const request = getRequestContext(source);

  const response = await request.delete(`${baseUrl}/api/v2/variables/${encodeURIComponent(key)}`, {
    timeout: 10_000,
  });

  // 404 = already deleted by another worker or cleanup; acceptable.
  if (response.status() === 404) {
    return;
  }

  if (!response.ok()) {
    const body = await response.text();

    throw new Error(`Variable deletion failed (${response.status()}): ${body}`);
  }
}

/** Cancel a single backfill via the API. 409 (already completed) is treated as success. */
export async function apiCancelBackfill(source: RequestLike, backfillId: number): Promise<void> {
  const request = getRequestContext(source);

  const response = await request.put(`${baseUrl}/api/v2/backfills/${backfillId}/cancel`, {
    timeout: 10_000,
  });

  if (response.status() !== 200 && response.status() !== 409) {
    throw new Error(`Cancel backfill failed (${response.status()})`);
  }
}

/** Cancel all active (non-completed) backfills for a DAG. */
export async function apiCancelAllActiveBackfills(source: RequestLike, dagId: string): Promise<void> {
  const request = getRequestContext(source);

  const response = await request.get(`${baseUrl}/api/v2/backfills?dag_id=${dagId}&limit=100`, {
    timeout: 10_000,
  });

  if (!response.ok()) {
    throw new Error(`List backfills failed (${response.status()})`);
  }

  const data = (await response.json()) as { backfills: Array<{ completed_at: string | null; id: number }> };

  for (const backfill of data.backfills) {
    if (backfill.completed_at === null) {
      await apiCancelBackfill(source, backfill.id);
    }
  }
}

/** Poll until all backfills for a DAG are completed. */
export async function apiWaitForNoActiveBackfill(
  source: RequestLike,
  dagId: string,
  timeout: number = 120_000,
): Promise<void> {
  const request = getRequestContext(source);

  await expect
    .poll(
      async () => {
        try {
          const response = await request.get(`${baseUrl}/api/v2/backfills?dag_id=${dagId}&limit=100`, {
            timeout: 10_000,
          });

          if (!response.ok()) {
            return false;
          }

          const data = (await response.json()) as {
            backfills: Array<{ completed_at: string | null }>;
          };

          return data.backfills.every((b) => b.completed_at !== null);
        } catch {
          return false;
        }
      },
      {
        intervals: [2000, 5000, 10_000],
        message: `Active backfills for DAG ${dagId} did not clear within ${timeout}ms`,
        timeout,
      },
    )
    .toBeTruthy();
}

/** Poll until a backfill reaches completed state. */
export async function apiWaitForBackfillComplete(
  source: RequestLike,
  backfillId: number,
  timeout: number = 120_000,
): Promise<void> {
  const request = getRequestContext(source);

  await expect
    .poll(
      async () => {
        try {
          const response = await request.get(`${baseUrl}/api/v2/backfills/${backfillId}`, {
            timeout: 10_000,
          });

          if (!response.ok()) {
            return false;
          }

          const data = (await response.json()) as { completed_at: string | null };

          return data.completed_at !== null;
        } catch {
          return false;
        }
      },
      {
        intervals: [2000, 5000, 10_000],
        message: `Backfill ${backfillId} did not complete within ${timeout}ms`,
        timeout,
      },
    )
    .toBeTruthy();
}

/** Create a backfill via the API. On 409, cancels active backfills and retries once. */
export async function apiCreateBackfill(
  source: RequestLike,
  dagId: string,
  options: {
    fromDate: string;
    maxActiveRuns?: number;
    reprocessBehavior?: string;
    toDate: string;
  },
): Promise<number> {
  const request = getRequestContext(source);
  const { fromDate, maxActiveRuns, reprocessBehavior = "none", toDate } = options;

  const body: Record<string, unknown> = {
    dag_id: dagId,
    from_date: fromDate,
    reprocess_behavior: reprocessBehavior,
    to_date: toDate,
  };

  if (maxActiveRuns !== undefined) {
    body.max_active_runs = maxActiveRuns;
  }

  const response = await request.post(`${baseUrl}/api/v2/backfills`, {
    data: body,
    headers: { "Content-Type": "application/json" },
    timeout: 10_000,
  });

  if (response.status() === 409) {
    await apiCancelAllActiveBackfills(source, dagId);
    await apiWaitForNoActiveBackfill(source, dagId, 30_000);

    const retryResponse = await request.post(`${baseUrl}/api/v2/backfills`, {
      data: body,
      headers: { "Content-Type": "application/json" },
      timeout: 10_000,
    });

    if (!retryResponse.ok()) {
      throw new Error(`Backfill creation retry failed (${retryResponse.status()})`);
    }

    return ((await retryResponse.json()) as { id: number }).id;
  }

  if (!response.ok()) {
    throw new Error(`Backfill creation failed (${response.status()})`);
  }

  return ((await response.json()) as { id: number }).id;
}

/**
 * Wait for a table (by testId) to load and show at least one row or an empty message.
 */
export async function waitForTableLoad(
  page: Page,
  options?: { checkSkeletons?: boolean; testId?: string; timeout?: number },
): Promise<void> {
  const testId = options?.testId ?? "table-list";
  const timeout = options?.timeout ?? 30_000;

  const table = page.getByTestId(testId);

  await expect(table).toBeVisible({ timeout });

  // Skip skeleton check by default — XComs uses [data-scope="skeleton"] for
  // lazy-loaded cell content, not table loading, which would cause false waits.
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
 * Wait for DOM row count to stabilize across consecutive measurements.
 * Uses 500ms intervals to give React concurrent rendering time to settle.
 */
export async function waitForStableRowCount(
  rowLocator: Locator,
  options?: { timeout?: number },
): Promise<number> {
  const timeout = options?.timeout ?? 10_000;
  const requiredStableChecks = 2;
  let lastStableCount = 0;

  await expect
    .poll(
      async () => {
        const counts: Array<number> = [];

        for (let i = 0; i < requiredStableChecks + 1; i++) {
          counts.push(await rowLocator.count());

          if (i < requiredStableChecks) {
            await new Promise((resolve) => setTimeout(resolve, 500));
          }
        }

        const first = counts[0] ?? 0;
        const allSame = counts.length > 0 && first > 0 && counts.every((c) => c === first);

        if (allSame) {
          lastStableCount = first;
        }

        return allSame;
      },
      { intervals: [500], timeout },
    )
    .toBe(true);

  return lastStableCount;
}
