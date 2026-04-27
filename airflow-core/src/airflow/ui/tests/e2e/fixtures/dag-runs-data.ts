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
 * DAG Runs page data fixture — creates runs across two DAGs for filtering tests.
 */
import { testConfig } from "playwright.config";
import { test as base } from "tests/e2e/fixtures";
import {
  apiCreateDagRun,
  apiSetDagRunState,
  safeCleanupDagRun,
  uniqueRunId,
  waitForDagReady,
} from "tests/e2e/utils/test-helpers";

export type DagRunsPageData = {
  dag1Id: string;
  dag2Id: string;
};

export const test = base.extend<Record<never, never>, { dagRunsPageData: DagRunsPageData }>({
  dagRunsPageData: [
    async ({ authenticatedRequest }, use, workerInfo) => {
      const dag1Id = testConfig.testDag.id;
      const dag2Id = "example_python_operator";
      const createdRuns: Array<{ dagId: string; runId: string }> = [];

      try {
        await Promise.all([
          waitForDagReady(authenticatedRequest, dag1Id),
          waitForDagReady(authenticatedRequest, dag2Id),
        ]);

        const baseOffset = workerInfo.parallelIndex * 7_200_000;
        const timestamp = Date.now() - baseOffset;

        const runId1 = uniqueRunId("dagrun_failed");

        await apiCreateDagRun(authenticatedRequest, dag1Id, {
          dag_run_id: runId1,
          logical_date: new Date(timestamp).toISOString(),
        });
        createdRuns.push({ dagId: dag1Id, runId: runId1 });
        await apiSetDagRunState(authenticatedRequest, { dagId: dag1Id, runId: runId1, state: "failed" });

        const runId2 = uniqueRunId("dagrun_success");

        await apiCreateDagRun(authenticatedRequest, dag1Id, {
          dag_run_id: runId2,
          logical_date: new Date(timestamp + 60_000).toISOString(),
        });
        createdRuns.push({ dagId: dag1Id, runId: runId2 });
        await apiSetDagRunState(authenticatedRequest, { dagId: dag1Id, runId: runId2, state: "success" });

        const runId3 = uniqueRunId("dagrun_other");

        await apiCreateDagRun(authenticatedRequest, dag2Id, {
          dag_run_id: runId3,
          logical_date: new Date(timestamp + 120_000).toISOString(),
        });
        createdRuns.push({ dagId: dag2Id, runId: runId3 });

        // eslint-disable-next-line react-hooks/rules-of-hooks
        await use({ dag1Id, dag2Id });
      } finally {
        for (const { dagId, runId } of createdRuns) {
          await safeCleanupDagRun(authenticatedRequest, dagId, runId);
        }
      }
    },
    { scope: "worker", timeout: 120_000 },
  ],
});
