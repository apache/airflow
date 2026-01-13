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
import { test, expect } from "@playwright/test";
import { testConfig } from "playwright.config";
import { DagsPage } from "tests/e2e/pages/DagsPage";
import { TaskInstancePage } from "tests/e2e/pages/TaskInstancePage";

test.describe("Task Instance Logs", () => {
    let dagsPage: DagsPage;
    let taskInstancePage: TaskInstancePage;
    const testDagId = testConfig.testDag.id;
    // Assuming the example DAG has a task named 'run_this_last' or similar standard task.
    // For 'example_bash_operator', 'run_this_last' is common, or 'runme_0'.
    // We'll dynamically find a task or use a known one. 
    // 'example_bash_operator' usually has 'runme_0', 'runme_1', etc.
    const testTaskId = "runme_0";

    test.beforeEach(({ page }) => {
        dagsPage = new DagsPage(page);
        taskInstancePage = new TaskInstancePage(page);
    });

    test("should verify task logs display", async () => {
        test.setTimeout(8 * 60 * 1000); // Longer timeout for DAG run and log collection

        // 1. Trigger DAG run
        const dagRunId = await dagsPage.triggerDag(testDagId);

        // 2. Wait for it to be running or success so logs are generated
        if (dagRunId) {
            await dagsPage.verifyDagRunStatus(testDagId, dagRunId);
        }

        // 3. Navigate to Task Instance Logs
        // We can use the helper method if we know the runId
        if (dagRunId) {
            await taskInstancePage.goto(testDagId, dagRunId, testTaskId);

            // 4. Verify Logs
            await taskInstancePage.waitForLogsToLoad();

            // Check for common log content
            // "Reading the config from" or "DEPENDENCIES_MET" or standard airflow logs
            // We'll check for generic log presence first
            await expect(taskInstancePage.logEntry.first()).toBeVisible();

            // 5. Verify Download Button
            await expect(taskInstancePage.downloadButton).toBeVisible();
            await expect(taskInstancePage.downloadButton).toBeEnabled();

            // 6. Verify Log Levels (optional explicit check if we can see colors/text)
            // Usually logs contain "[INFO]"
            await taskInstancePage.verifyLogContent("[INFO]");
        }
    });
});
