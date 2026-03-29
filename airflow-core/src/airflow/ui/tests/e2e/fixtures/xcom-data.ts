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
 * XCom data fixture — triggers example_xcom DAG runs to generate XCom entries.
 */

/* eslint-disable react-hooks/rules-of-hooks -- Playwright's `use` is not a React Hook. */
import { testConfig } from "playwright.config";
import { test as base } from "tests/e2e/fixtures";
import {
  apiCreateDagRun,
  safeCleanupDagRun,
  uniqueRunId,
  waitForDagReady,
  waitForDagRunStatus,
} from "tests/e2e/utils/test-helpers";

export type XcomRunsData = {
  dagId: string;
  xcomKey: string;
};

export const test = base.extend<Record<never, never>, { xcomRunsData: XcomRunsData }>({
  xcomRunsData: [
    async ({ authenticatedRequest }, use, workerInfo) => {
      const dagId = testConfig.xcomDag.id;
      const createdRunIds: Array<string> = [];
      const triggerCount = 2;
      const baseOffset = workerInfo.parallelIndex * 7_200_000;

      try {
        await waitForDagReady(authenticatedRequest, dagId);
        await authenticatedRequest.patch(`/api/v2/dags/${dagId}`, {
          data: { is_paused: false },
        });

        for (let i = 0; i < triggerCount; i++) {
          const runId = uniqueRunId(`xcom_run_${i}`);

          await apiCreateDagRun(authenticatedRequest, dagId, {
            dag_run_id: runId,
            logical_date: new Date(Date.now() - baseOffset + i * 60_000).toISOString(),
          });
          createdRunIds.push(runId);
          await waitForDagRunStatus(authenticatedRequest, {
            dagId,
            expectedState: "success",
            runId,
            timeout: 120_000,
          });
        }

        await use({ dagId, xcomKey: "return_value" });
      } finally {
        for (const runId of createdRunIds) {
          await safeCleanupDagRun(authenticatedRequest, dagId, runId);
        }
      }
    },
    { scope: "worker", timeout: 180_000 },
  ],
});
