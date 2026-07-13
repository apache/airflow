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
 * Backfill API helpers — create, cancel, poll completion.
 */
import { expect } from "@playwright/test";

import { baseUrl, getRequestContext, type RequestLike } from "../shared";

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

/** Cancel all active (non-completed) backfills for a Dag. */
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

/** Poll until all backfills for a Dag are completed. */
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
        message: `Active backfills for Dag ${dagId} did not clear within ${timeout}ms`,
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
