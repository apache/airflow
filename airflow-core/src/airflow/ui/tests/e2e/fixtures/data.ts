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
 * Data-isolation fixtures for E2E tests.
 *
 * Extends the POM fixtures with worker-scoped and test-scoped data
 * fixtures that create API resources and guarantee cleanup — even on
 * test failure, timeout, or retry.
 */

/* eslint-disable react-hooks/rules-of-hooks -- Playwright's `use` is not a React Hook. */
import type { APIRequestContext } from "@playwright/test";

import { testConfig } from "../../../playwright.config";
import {
  apiCreateDagRun,
  apiSetDagRunState,
  apiTriggerDagRun,
  safeCleanupDagRun,
  uniqueRunId,
  waitForDagReady,
  waitForDagRunStatus,
} from "../utils/test-helpers";
import { test as base } from "./pom";

/** Shape returned by single DAG run fixtures. */
export type DagRunFixtureData = {
  dagId: string;
  logicalDate: string;
  runId: string;
};

/** Shape returned by the successAndFailedRuns fixture. */
export type SuccessAndFailedRunsData = {
  dagId: string;
  failedRun: DagRunFixtureData;
  successRun: DagRunFixtureData;
};

export type DataWorkerFixtures = {
  /** Ensures the default test DAG is parsed and ready. Worker-scoped, no cleanup needed. */
  dagReady: string;
  /** A DAG run triggered via scheduler and completed. Worker-scoped with auto-cleanup. */
  executedDagRun: DagRunFixtureData;
  /** Two DAG runs: one success, one failed. Worker-scoped with auto-cleanup. */
  successAndFailedRuns: SuccessAndFailedRunsData;
  /** A DAG run in "success" state (API-only, no scheduler). Worker-scoped with auto-cleanup. */
  successDagRun: DagRunFixtureData;
};

export type DataTestFixtures = Record<never, never>;

async function createAndSetupDagRun(
  request: APIRequestContext,
  dagId: string,
  options: {
    logicalDate?: string;
    parallelIndex: number;
    prefix: string;
    state: "failed" | "queued" | "success";
  },
): Promise<DagRunFixtureData> {
  await waitForDagReady(request, dagId);

  const runId = uniqueRunId(`${options.prefix}_w${options.parallelIndex}`);

  // Offset logical_date by 2 hours per worker to avoid collisions.
  const offsetMs = options.parallelIndex * 7_200_000;
  const logicalDate = options.logicalDate ?? new Date(Date.now() - offsetMs).toISOString();

  const actualRunId = await apiCreateDagRun(request, dagId, {
    dag_run_id: runId,
    logical_date: logicalDate,
  });

  await apiSetDagRunState(request, { dagId, runId: actualRunId, state: options.state });

  return { dagId, logicalDate, runId: actualRunId };
}

async function cleanupMultipleRuns(
  request: APIRequestContext,
  runs: Array<{ dagId: string; runId: string }>,
): Promise<void> {
  await Promise.all(runs.map(({ dagId, runId }) => safeCleanupDagRun(request, dagId, runId)));
}

export const test = base.extend<DataTestFixtures, DataWorkerFixtures>({
  dagReady: [
    async ({ authenticatedRequest }, use) => {
      const dagId = testConfig.testDag.id;

      await waitForDagReady(authenticatedRequest, dagId);
      await use(dagId);
    },
    { scope: "worker", timeout: 120_000 },
  ],

  executedDagRun: [
    async ({ authenticatedRequest }, use) => {
      const dagId = testConfig.testDag.id;
      let dagRunId: string | undefined;

      try {
        await waitForDagReady(authenticatedRequest, dagId);
        await authenticatedRequest.patch(`/api/v2/dags/${dagId}`, {
          data: { is_paused: false },
        });

        const triggered = await apiTriggerDagRun(authenticatedRequest, dagId);

        ({ dagRunId } = triggered);

        await waitForDagRunStatus(authenticatedRequest, {
          dagId,
          expectedState: "success",
          runId: dagRunId,
          timeout: 120_000,
        });

        await use({ dagId, logicalDate: triggered.logicalDate, runId: dagRunId });
      } finally {
        if (dagRunId !== undefined) {
          await safeCleanupDagRun(authenticatedRequest, dagId, dagRunId);
        }
        // Re-pause is handled by global-teardown.ts (globalTeardown in playwright.config.ts).
      }
    },
    { scope: "worker", timeout: 180_000 },
  ],

  successAndFailedRuns: [
    async ({ authenticatedRequest }, use, workerInfo) => {
      const dagId = testConfig.testDag.id;
      const createdRuns: Array<{ dagId: string; runId: string }> = [];

      try {
        await waitForDagReady(authenticatedRequest, dagId);

        const baseOffset = workerInfo.parallelIndex * 7_200_000;
        const timestamp = Date.now() - baseOffset;

        const successRun = await createAndSetupDagRun(authenticatedRequest, dagId, {
          logicalDate: new Date(timestamp).toISOString(),
          parallelIndex: workerInfo.parallelIndex,
          prefix: "sf_ok",
          state: "success",
        });

        createdRuns.push({ dagId, runId: successRun.runId });

        const failedRun = await createAndSetupDagRun(authenticatedRequest, dagId, {
          logicalDate: new Date(timestamp + 60_000).toISOString(),
          parallelIndex: workerInfo.parallelIndex,
          prefix: "sf_fail",
          state: "failed",
        });

        createdRuns.push({ dagId, runId: failedRun.runId });

        await use({ dagId, failedRun, successRun });
      } finally {
        await cleanupMultipleRuns(authenticatedRequest, createdRuns);
      }
    },
    { scope: "worker", timeout: 120_000 },
  ],

  successDagRun: [
    async ({ authenticatedRequest }, use, workerInfo) => {
      const dagId = testConfig.testDag.id;
      let createdRunId: string | undefined;

      try {
        const data = await createAndSetupDagRun(authenticatedRequest, dagId, {
          parallelIndex: workerInfo.parallelIndex,
          prefix: "run",
          state: "success",
        });

        createdRunId = data.runId;

        await use(data);
      } finally {
        if (createdRunId !== undefined) {
          await safeCleanupDagRun(authenticatedRequest, dagId, createdRunId);
        }
      }
    },
    { scope: "worker", timeout: 120_000 },
  ],
});
