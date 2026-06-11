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
 * Task instances data fixture — creates runs with success/failed task instances.
 */
import { expect, type APIRequestContext } from "@playwright/test";
import { testConfig } from "playwright.config";
import { test as base } from "tests/e2e/fixtures";
import {
  apiCreateDagRun,
  safeCleanupDagRun,
  uniqueRunId,
  waitForDagReady,
} from "tests/e2e/utils/test-helpers";

export type TaskInstancesData = {
  dagId: string;
};

async function setAllTaskInstanceStates(
  request: APIRequestContext,
  options: { dagId: string; runId: string; state: string },
): Promise<void> {
  const { dagId, runId, state } = options;

  const tasksResponse = await request.get(`/api/v2/dags/${dagId}/dagRuns/${runId}/taskInstances`);

  expect(tasksResponse.ok()).toBeTruthy();

  const tasksData = (await tasksResponse.json()) as {
    task_instances: Array<{ task_id: string }>;
  };

  for (const task of tasksData.task_instances) {
    await expect
      .poll(
        async () => {
          const resp = await request.patch(
            `/api/v2/dags/${dagId}/dagRuns/${runId}/taskInstances/${task.task_id}`,
            {
              data: { new_state: state },
              headers: { "Content-Type": "application/json" },
              timeout: 30_000,
            },
          );

          return resp.ok();
        },
        { intervals: [2000], timeout: 30_000 },
      )
      .toBe(true);
  }
}

export const test = base.extend<Record<never, never>, { taskInstancesData: TaskInstancesData }>({
  taskInstancesData: [
    async ({ authenticatedRequest }, use, workerInfo) => {
      const dagId = testConfig.testDag.id;
      const createdRunIds: Array<string> = [];
      const baseOffset = workerInfo.parallelIndex * 7_200_000;
      const timestamp = Date.now() - baseOffset;

      try {
        await waitForDagReady(authenticatedRequest, dagId);

        const runId1 = uniqueRunId("ti_success");

        await apiCreateDagRun(authenticatedRequest, dagId, {
          dag_run_id: runId1,
          logical_date: new Date(timestamp).toISOString(),
        });
        createdRunIds.push(runId1);
        await setAllTaskInstanceStates(authenticatedRequest, { dagId, runId: runId1, state: "success" });

        const runId2 = uniqueRunId("ti_failed");

        await apiCreateDagRun(authenticatedRequest, dagId, {
          dag_run_id: runId2,
          logical_date: new Date(timestamp + 60_000).toISOString(),
        });
        createdRunIds.push(runId2);
        await setAllTaskInstanceStates(authenticatedRequest, { dagId, runId: runId2, state: "failed" });

        // eslint-disable-next-line react-hooks/rules-of-hooks
        await use({ dagId });
      } finally {
        for (const runId of createdRunIds) {
          await safeCleanupDagRun(authenticatedRequest, dagId, runId);
        }
      }
    },
    { scope: "worker", timeout: 120_000 },
  ],
});
