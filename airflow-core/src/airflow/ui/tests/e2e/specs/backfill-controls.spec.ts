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
import { testConfig, AUTH_FILE } from "playwright.config";
import { BackfillPage } from "tests/e2e/pages/BackfillPage";

const getPastDate = (daysAgo: number): string => {
  const date = new Date();

  date.setDate(date.getDate() - daysAgo);
  date.setHours(0, 0, 0, 0);

  return date.toISOString().slice(0, 16);
};

test.describe("Backfill Controls", () => {
  const testDagId = testConfig.testDag.id;
  const fromDate = getPastDate(2);
  const toDate = getPastDate(1);

  test("should pause and resume a running backfill", async ({ page }) => {
    test.setTimeout(180_000);
    const backfillPage = new BackfillPage(page);

    await backfillPage.navigateToDagDetail(testDagId);
    await backfillPage.openBackfillDialog();

    await backfillPage.backfillFromDateInput.fill(fromDate);
    await backfillPage.backfillToDateInput.fill(toDate);
    await backfillPage.selectReprocessBehavior("All Runs");

    const runsMessage = page.locator("text=/\\d+ runs? will be triggered|No runs matching/");

    await expect(runsMessage).toBeVisible({ timeout: 10_000 });
    await expect(backfillPage.backfillRunButton).toBeEnabled({ timeout: 15_000 });
    await backfillPage.backfillRunButton.click();

    await backfillPage.navigateToBackfillsTab(testDagId);
    await expect(backfillPage.pauseButton).toBeVisible({ timeout: 10_000 });

    await backfillPage.clickPauseButton();

    const isPaused = await backfillPage.isBackfillPaused();

    expect(isPaused).toBe(true);

    await backfillPage.clickPauseButton();

    const isResumed = await backfillPage.isBackfillPaused();

    expect(isResumed).toBe(false);
  });

  test("should cancel an active backfill", async ({ page }) => {
    test.setTimeout(180_000);
    const backfillPage = new BackfillPage(page);

    await backfillPage.navigateToDagDetail(testDagId);
    await backfillPage.openBackfillDialog();

    await backfillPage.backfillFromDateInput.fill(fromDate);
    await backfillPage.backfillToDateInput.fill(toDate);
    await backfillPage.selectReprocessBehavior("All Runs");

    const runsMessage = page.locator("text=/\\d+ runs? will be triggered|No runs matching/");

    await expect(runsMessage).toBeVisible({ timeout: 10_000 });
    await expect(backfillPage.backfillRunButton).toBeEnabled({ timeout: 15_000 });
    await backfillPage.backfillRunButton.click();

    await backfillPage.navigateToBackfillsTab(testDagId);
    await expect(backfillPage.cancelButton).toBeVisible({ timeout: 10_000 });

    await backfillPage.clickCancelButton();

    await expect(backfillPage.pauseButton).not.toBeVisible({ timeout: 10_000 });
    await expect(backfillPage.cancelButton).not.toBeVisible({ timeout: 10_000 });
  });

  test("should not be able to resume a cancelled backfill", async ({ page }) => {
    test.setTimeout(180_000);
    const backfillPage = new BackfillPage(page);

    await backfillPage.navigateToDagDetail(testDagId);
    await backfillPage.openBackfillDialog();

    await backfillPage.backfillFromDateInput.fill(fromDate);
    await backfillPage.backfillToDateInput.fill(toDate);
    await backfillPage.selectReprocessBehavior("All Runs");

    const runsMessage = page.locator("text=/\\d+ runs? will be triggered|No runs matching/");

    await expect(runsMessage).toBeVisible({ timeout: 10_000 });
    await expect(backfillPage.backfillRunButton).toBeEnabled({ timeout: 15_000 });
    await backfillPage.backfillRunButton.click();

    await backfillPage.navigateToBackfillsTab(testDagId);
    await expect(backfillPage.cancelButton).toBeVisible({ timeout: 10_000 });

    await backfillPage.clickCancelButton();

    await expect(backfillPage.pauseButton).not.toBeVisible({ timeout: 10_000 });
    await expect(backfillPage.cancelButton).not.toBeVisible({ timeout: 10_000 });
  });
});
