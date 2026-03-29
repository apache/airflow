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
import type { Page } from "@playwright/test";

const HEALTH_ENDPOINT = "/api/v2/monitor/health";
const MAX_WAIT_MS = 30_000;
const REQUEST_TIMEOUT_MS = 10_000;
const BACKOFF_INTERVALS = [1000, 2000, 4000, 8000];

/**
 * Wait for the Airflow server to be responsive before proceeding.
 *
 * Polls the health endpoint, checking for HTTP 200. Uses backoff intervals between retries.
 * We intentionally do not check response time: on Firefox/CI the health endpoint regularly
 * exceeds 2s, which would cause the check to never succeed.
 *
 * When the server responds on the first attempt, this function returns immediately.
 */
export async function waitForServerReady(page: Page): Promise<void> {
  const startTime = Date.now();
  let attempt = 0;

  while (Date.now() - startTime < MAX_WAIT_MS) {
    try {
      const response = await page.request.get(HEALTH_ENDPOINT, {
        timeout: REQUEST_TIMEOUT_MS,
      });

      if (response.status() === 200) {
        return;
      }
    } catch {
      // Request failed or timed out — server not ready yet.
    }

    const index = Math.min(attempt, BACKOFF_INTERVALS.length - 1);
    const interval = BACKOFF_INTERVALS[index] as number;
    const remaining = MAX_WAIT_MS - (Date.now() - startTime);

    if (remaining <= 0) {
      break;
    }

    await page.waitForTimeout(Math.min(interval, remaining));
    attempt++;
  }

  throw new Error(
    `Server not ready after ${MAX_WAIT_MS}ms — health endpoint ${HEALTH_ENDPOINT} did not return 200`,
  );
}
