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
import { expect, test } from "@playwright/test";
import { AUTH_FILE, testConfig } from "playwright.config";

import { TaskInstancePage } from "../pages/TaskInstancePage";

test.describe("Verify task logs display", () => {
  test.describe.configure({ mode: "serial" });

  const testDagId = testConfig.testDag.id;
  const testTaskId = testConfig.testTask.id;

  let dagRunId: string;

  test.beforeAll(async ({ browser }) => {
    test.setTimeout(120_000);

    const context = await browser.newContext({ storageState: AUTH_FILE });
    const page = await context.newPage();
    const taskInstancePage = new TaskInstancePage(page);

    await taskInstancePage.triggerDagAndWaitForSuccess(testDagId);

    const url = page.url();
    const match = /runs\/([^/]+)/.exec(url);

    dagRunId = match?.[1] ?? "";

    if (!dagRunId) {
      throw new Error(`Could not extract dagRunId from URL: ${url}`);
    }

    await context.close();
  });

  test.beforeEach(async ({ page }) => {
    const taskInstancePage = new TaskInstancePage(page);

    await taskInstancePage.navigateToTaskInstance(testDagId, dagRunId, testTaskId);
  });

  test("Verify log content is displayed", async ({ page }) => {
    const virtualizedList = page.locator('[data-testid="virtualized-list"]');

    await expect(virtualizedList).toBeVisible({ timeout: 30_000 });
    const logItems = page.locator('[data-testid^="virtualized-item-"]');

    await expect(logItems.first()).toBeVisible({ timeout: 10_000 });
  });

  test("Verify log levels are visible", async ({ page }) => {
    const virtualizedList = page.locator('[data-testid="virtualized-list"]');

    await expect(virtualizedList).toBeVisible({ timeout: 30_000 });

    await expect(virtualizedList).toContainText(/INFO|WARNING|ERROR|CRITICAL/);
  });

  test("Verify log timestamp formatting", async ({ page }) => {
    const virtualizedList = page.locator('[data-testid="virtualized-list"]');

    await expect(virtualizedList).toBeVisible({ timeout: 30_000 });

    await expect(virtualizedList).toContainText(/\[\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}]/);
  });

  test("Verify log settings", async ({ page }) => {
    const virtualizedList = page.locator('[data-testid="virtualized-list"]');

    await expect(virtualizedList).toBeVisible({ timeout: 30_000 });

    // Verify timestamps are visible initially
    await expect(virtualizedList).toContainText(/\[\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}]/);

    await page.getByTestId("log-settings-button").click();
    await page.getByTestId("log-settings-timestamp").click();
    await expect(virtualizedList).not.toContainText(/\[\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}]/);

    // Test Show Source
    await page.getByTestId("log-settings-button").click();
    await page.getByTestId("log-settings-source").click();
    await expect(virtualizedList).toContainText(/source/);

    // Test Unwrap
    await page.getByTestId("log-settings-button").click();
    const wrapMenuItem = page.getByTestId("log-settings-wrap");

    await expect(wrapMenuItem).toContainText(/Wrap|Unwrap/);
    await wrapMenuItem.click();

    // Test Expand
    await page.getByTestId("log-settings-button").click();
    const expandMenuItem = page.getByTestId("log-settings-expand");

    await expect(expandMenuItem).toContainText(/Expand|Collapse/);
    await expandMenuItem.click();
  });

  test("Verify logs are getting downloaded fine", async ({ page }) => {
    const virtualizedList = page.locator('[data-testid="virtualized-list"]');

    await expect(virtualizedList).toBeVisible({ timeout: 30_000 });

    const downloadPromise = page.waitForEvent("download", { timeout: 10_000 });

    await page.getByTestId("download-logs-button").click();
    const download = await downloadPromise;

    expect(download.suggestedFilename()).toMatch(/^logs_.*\.txt$/);
  });
});
