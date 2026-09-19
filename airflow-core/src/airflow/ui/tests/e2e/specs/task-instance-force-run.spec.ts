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
import { expect, test } from "tests/e2e/fixtures";
import {
  apiTriggerDagRun,
  safeCleanupDagRun,
  waitForDagRunStatus,
  waitForTaskInstanceState,
} from "tests/e2e/utils/api/dag-runs";
import { baseUrl, uniqueRunId } from "tests/e2e/utils/shared";

// example_short_circuit_operator's condition_is_False task short-circuits, leaving false_1
// skipped — a real blocked instance that only a force run (not a plain clear) can re-run.
const dagId = "example_short_circuit_operator";
const taskId = "false_1";

let runId: string;

test.describe("Force run a task instance", () => {
  test.beforeAll(async ({ authenticatedRequest }) => {
    test.setTimeout(600_000);

    ({ dagRunId: runId } = await apiTriggerDagRun(authenticatedRequest, dagId, {
      runId: uniqueRunId("force_run"),
    }));

    await waitForDagRunStatus(authenticatedRequest, {
      dagId,
      expectedState: "success",
      runId,
      timeout: 180_000,
    });
    await waitForTaskInstanceState(authenticatedRequest, {
      dagId,
      expectedState: "skipped",
      runId,
      taskId,
    });
  });

  test.afterAll(async ({ authenticatedRequest }) => {
    await safeCleanupDagRun(authenticatedRequest, dagId, runId);
  });

  test("re-runs a skipped task instance and records the flag", async ({
    authenticatedRequest,
    page,
    taskInstancePage,
  }) => {
    test.slow();

    await taskInstancePage.navigateToTaskInstance(dagId, runId, taskId);
    await taskInstancePage.forceRun();

    // Proof the scheduler actually ran the forced task, not just that the UI accepted the click.
    await waitForTaskInstanceState(authenticatedRequest, {
      dagId,
      expectedState: "success",
      runId,
      taskId,
      timeout: 180_000,
    });

    const response = await authenticatedRequest.get(
      `${baseUrl}/api/v2/dags/${dagId}/dagRuns/${runId}/taskInstances/${taskId}`,
    );
    const json = (await response.json()) as { ignore_upstream_deps: boolean };

    expect(json.ignore_upstream_deps).toBe(true);

    await taskInstancePage.navigateToTaskInstanceDetails(dagId, runId, taskId);
    await expect(page.locator("#details-panel")).toContainText("Force Run", { timeout: 30_000 });
  });
});
