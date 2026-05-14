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
 * Dashboard data fixture — tracks UI-triggered DAG runs for cleanup.
 */
import { testConfig } from "playwright.config";
import { test as base } from "tests/e2e/fixtures";
import { safeCleanupDagRun } from "tests/e2e/utils/test-helpers";

export type DagRunCleanup = {
  track: (runId: string) => void;
};

/* eslint-disable react-hooks/rules-of-hooks -- Playwright's `use` is not a React Hook. */
export const test = base.extend<{ dagRunCleanup: DagRunCleanup }>({
  dagRunCleanup: async ({ authenticatedRequest }, use) => {
    const trackedRunIds: Array<string> = [];

    await use({ track: (runId: string) => trackedRunIds.push(runId) });

    for (const runId of trackedRunIds) {
      await safeCleanupDagRun(authenticatedRequest, testConfig.testDag.id, runId);
    }
  },
});
