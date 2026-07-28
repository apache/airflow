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
 * UI wait helpers for DOM stability and concurrent rendering.
 */
import { expect, type Locator } from "@playwright/test";

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
