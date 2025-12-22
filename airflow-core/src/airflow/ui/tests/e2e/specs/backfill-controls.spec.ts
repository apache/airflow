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
import { testConfig } from "playwright.config";
import { BackfillPage } from "tests/e2e/pages/BackfillPage";

test.describe("Backfill Controls", () => {
  let backfillPage: BackfillPage;

  const testDagId = testConfig.testDag.id;

  test.beforeEach(({ page }) => {
    backfillPage = new BackfillPage(page);
  });

  test("should pause and resume a running backfill", async () => {
    await backfillPage.navigateToBackfillsTab(testDagId);

    await backfillPage.createBackfill(testDagId);

    await backfillPage.clickPauseButton();

    const isPaused = await backfillPage.isBackfillPaused();

    expect(isPaused).toBe(true);

    await backfillPage.clickPauseButton();

    const isRunning = await backfillPage.isBackfillPaused();

    expect(isRunning).toBe(false);
  });

  test("should cancel an active backfill", async () => {
    await backfillPage.navigateToBackfillsTab(testDagId);

    await backfillPage.createBackfill(testDagId);

    await backfillPage.clickCancelButton();

    await backfillPage.waitForBackfillCompletion();

    await expect(backfillPage.backfillBanner).not.toBeVisible();
  });

  test("should not be able to resume a cancelled backfill", async () => {
    await backfillPage.navigateToBackfillsTab(testDagId);

    await backfillPage.createBackfill(testDagId);

    await backfillPage.clickCancelButton();

    await backfillPage.waitForBackfillCompletion();

    await expect(backfillPage.pauseButton).not.toBeVisible();
    await expect(backfillPage.cancelButton).not.toBeVisible();
  });
});
