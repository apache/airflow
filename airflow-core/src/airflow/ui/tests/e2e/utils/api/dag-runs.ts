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
 * DAG run API helpers — trigger, create, set state, poll, delete, cleanup.
 * Includes task-instance state polling since TIs belong to a DAG run.
 */
import { expect } from "@playwright/test";

import { baseUrl, getRequestContext, type RequestLike, uniqueRunId } from "../shared";

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

/**
 * Poll GET /api/v2/dags/{dagId} until 200 — waits for Dag to be parsed.
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
 * Trigger a Dag run with an auto-generated unique run ID.
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

      throw new Error(`Dag run trigger failed (${response.status()})`);
    }

    const json = (await response.json()) as { logical_date?: string } & DagRunResponse;

    resultLogicalDate = json.logical_date ?? logicalDate;
  }).toPass({ intervals: [2000, 3000, 5000], timeout: 60_000 });

  return { dagRunId, logicalDate: resultLogicalDate };
}

/**
 * Create a Dag run via the API.
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

      throw new Error(`Dag run creation failed (${response.status()})`);
    }

    const json = (await response.json()) as DagRunResponse;

    resultRunId = json.dag_run_id;
  }).toPass({ intervals: [2000, 3000, 5000], timeout: 60_000 });

  return resultRunId;
}

/**
 * Set a Dag run's state via the API.
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
      throw new Error(`Set Dag run state failed (${response.status()})`);
    }
  }).toPass({ intervals: [2000, 3000, 5000], timeout: 60_000 });
}

/**
 * Poll the API until the Dag run reaches the expected state.
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
            throw new Error(`Dag run ${runId} failed unexpectedly`);
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
        message: `Dag run ${runId} did not reach state "${expectedState}" within ${timeout}ms`,
        timeout,
      },
    )
    .toBe(expectedState);
}

/**
 * Poll the API until a task instance reaches the expected state.
 * Unlike waitForDagRunStatus, this targets a specific task within a Dag run.
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

/** Delete a Dag run via the API. 404 is treated as success. */
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

    throw new Error(`Dag run deletion failed (${response.status()}): ${body}`);
  }
}

/**
 * Delete a Dag run, logging (not throwing) unexpected errors.
 * Use this in fixture teardown where cleanup must not abort the loop.
 * 404 is already handled inside `apiDeleteDagRun`.
 *
 * Strategy: force-fail the run first so the server doesn't wait for
 * running tasks during deletion, then delete with one retry on timeout.
 */
export async function safeCleanupDagRun(source: RequestLike, dagId: string, runId: string): Promise<void> {
  try {
    await apiSetDagRunState(source, { dagId, runId, state: "failed" });
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

      console.warn(`[e2e cleanup] Failed to delete Dag run ${dagId}/${runId}: ${message}`);

      return;
    }
  }
}
