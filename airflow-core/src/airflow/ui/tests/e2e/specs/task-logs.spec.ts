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
import { testConfig } from "playwright.config";

import { expect, test } from "tests/e2e/fixtures";

test.describe("Verify task logs display", () => {
  const testTaskId = testConfig.testTask.id;

  test.beforeEach(async ({ executedDagRun, taskInstancePage }) => {
    await taskInstancePage.navigateToTaskInstance(executedDagRun.dagId, executedDagRun.runId, testTaskId);
  });

  test("Verify log content is displayed", async ({ executedDagRun: _run, page }) => {
    const virtualizedList = page.getByTestId("virtualized-list");

    await expect(virtualizedList).toBeVisible({ timeout: 30_000 });

    const logItems = page.getByTestId(/^virtualized-item-/);

    await expect(logItems.first()).toBeVisible({ timeout: 30_000 });
  });

  test("Verify logs are getting downloaded fine", async ({ executedDagRun: _run, page }) => {
    const virtualizedList = page.getByTestId("virtualized-list");

    await expect(virtualizedList).toBeVisible({ timeout: 30_000 });

    const downloadButton = page.getByTestId("download-logs-button");

    await expect(downloadButton).toBeVisible();
    await expect(downloadButton).toBeEnabled();

    const downloadPromise = page.waitForEvent("download", { timeout: 30_000 });

    await downloadButton.click();
    const download = await downloadPromise;

    expect(download.suggestedFilename()).toMatch(/^logs_.*\.txt$/);
  });
});
