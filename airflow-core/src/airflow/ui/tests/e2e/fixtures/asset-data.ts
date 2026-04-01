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
 * Asset data fixture — triggers asset_produces_1 DAG and waits for success.
 */

/* eslint-disable react-hooks/rules-of-hooks -- Playwright's `use` is not a React Hook. */
import { test as base } from "tests/e2e/fixtures";
import {
  safeCleanupDagRun,
  apiTriggerDagRun,
  waitForDagReady,
  waitForDagRunStatus,
} from "tests/e2e/utils/test-helpers";

export type AssetData = {
  dagId: string;
};

export const test = base.extend<Record<never, never>, { assetData: AssetData }>({
  assetData: [
    async ({ authenticatedRequest }, use, _workerInfo) => {
      const dagId = "asset_produces_1";
      let createdRunId: string | undefined;

      try {
        await waitForDagReady(authenticatedRequest, dagId);
        await authenticatedRequest.patch(`/api/v2/dags/${dagId}`, { data: { is_paused: false } });
        const { dagRunId } = await apiTriggerDagRun(authenticatedRequest, dagId);

        createdRunId = dagRunId;

        await waitForDagRunStatus(authenticatedRequest, {
          dagId,
          expectedState: "success",
          runId: dagRunId,
          timeout: 120_000,
        });

        await use({ dagId });
      } finally {
        if (createdRunId !== undefined) {
          await safeCleanupDagRun(authenticatedRequest, dagId, createdRunId);
        }
      }
    },
    { scope: "worker", timeout: 180_000 },
  ],
});
