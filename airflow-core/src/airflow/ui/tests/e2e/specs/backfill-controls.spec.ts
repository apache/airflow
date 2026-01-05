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

  test.beforeAll(async ({ browser }) => {
    test.setTimeout(180_000);

    const context = await browser.newContext({ storageState: AUTH_FILE });
    const page = await context.newPage();
    const setupBackfillPage = new BackfillPage(page);

    await setupBackfillPage.createBackfill(testDagId, {
      fromDate,
      reprocessBehavior: "All Runs",
      toDate,
    });

    await context.close();
  });

  test("should pause and resume a running backfill", async ({ page }) => {
    const backfillPage = new BackfillPage(page);

    await backfillPage.navigateToBackfillsTab(testDagId);

    await backfillPage.clickPauseButton();

    const pausedStatus = await backfillPage.getBackfillStatus(0);

    expect(pausedStatus.toLowerCase()).toContain("paused");

    await backfillPage.clickPauseButton();

    const runningStatus = await backfillPage.getBackfillStatus(0);

    expect(runningStatus.toLowerCase()).toContain("running");
  });

  test("should cancel an active backfill", async ({ page }) => {
    const backfillPage = new BackfillPage(page);

    await backfillPage.navigateToBackfillsTab(testDagId);

    await backfillPage.createBackfill(testDagId, {
      fromDate,
      reprocessBehavior: "All Runs",
      toDate,
    });

    await backfillPage.clickCancelButton();

    await backfillPage.waitForBackfillCompletion();

    const cancelledStatus = await backfillPage.getBackfillStatus(0);

    expect(cancelledStatus.toLowerCase()).toContain("cancel");

    await expect(backfillPage.backfillBanner).not.toBeVisible();
  });

  test("should not be able to resume a cancelled backfill", async ({ page }) => {
    const backfillPage = new BackfillPage(page);

    await backfillPage.navigateToBackfillsTab(testDagId);

    await backfillPage.createBackfill(testDagId, {
      fromDate,
      reprocessBehavior: "All Runs",
      toDate,
    });

    await backfillPage.clickCancelButton();

    await backfillPage.waitForBackfillCompletion();

    await expect(backfillPage.pauseButton).not.toBeVisible();
    await expect(backfillPage.cancelButton).not.toBeVisible();
  });
});
