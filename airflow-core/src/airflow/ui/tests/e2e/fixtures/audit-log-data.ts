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
 * Audit log data fixture — triggers DAG runs to generate audit log entries.
 */

/* eslint-disable react-hooks/rules-of-hooks -- Playwright's `use` is not a React Hook. */
import { testConfig } from "playwright.config";
import { test as base } from "tests/e2e/fixtures";
import {
  safeCleanupDagRun,
  apiTriggerDagRun,
  waitForDagReady,
  waitForDagRunStatus,
} from "tests/e2e/utils/test-helpers";

export type AuditLogData = {
  dagId: string;
};

export const test = base.extend<Record<never, never>, { auditLogData: AuditLogData }>({
  auditLogData: [
    async ({ authenticatedRequest }, use, _workerInfo) => {
      const dagId = testConfig.testDag.id;
      const createdRunIds: Array<string> = [];

      try {
        await waitForDagReady(authenticatedRequest, dagId);
        await authenticatedRequest.patch(`/api/v2/dags/${dagId}`, { data: { is_paused: false } });

        for (let i = 0; i < 3; i++) {
          const { dagRunId } = await apiTriggerDagRun(authenticatedRequest, dagId);

          createdRunIds.push(dagRunId);
          await waitForDagRunStatus(authenticatedRequest, {
            dagId,
            expectedState: "success",
            runId: dagRunId,
            timeout: 60_000,
          });
        }

        await use({ dagId });
      } finally {
        for (const runId of createdRunIds) {
          await safeCleanupDagRun(authenticatedRequest, dagId, runId);
        }
      }
    },
    { scope: "worker", timeout: 180_000 },
  ],
});
