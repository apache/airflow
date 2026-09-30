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
import { expect, test as base, type DagRunFixtureData } from "tests/e2e/fixtures";
import {
  apiTriggerDagRun,
  safeCleanupDagRun,
  waitForDagReady,
  waitForDagRunStatus,
} from "tests/e2e/utils/api/dag-runs";

export const test = base.extend<{ tryDagId: string; tryRun: DagRunFixtureData }>({
  tryDagId: ["example_task_state_store", { option: true }],
  tryRun: async ({ authenticatedRequest, tryDagId: dagId }, use) => {
    await waitForDagReady(authenticatedRequest, dagId);
    const unpause = await authenticatedRequest.patch(`/api/v2/dags/${dagId}`, {
      data: { is_paused: false },
    });

    await expect(unpause).toBeOK();
    const { dagRunId: runId, logicalDate } = await apiTriggerDagRun(authenticatedRequest, dagId);

    try {
      await waitForDagRunStatus(authenticatedRequest, { dagId, expectedState: "success", runId });
      // eslint-disable-next-line react-hooks/rules-of-hooks -- Playwright fixture
      await use({ dagId, logicalDate, runId });
    } finally {
      await safeCleanupDagRun(authenticatedRequest, dagId, runId);
    }
  },
});
