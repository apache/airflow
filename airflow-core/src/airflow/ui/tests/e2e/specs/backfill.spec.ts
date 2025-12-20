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
import { LoginPage } from "tests/e2e/pages/LoginPage";

/**
 * Backfill E2E Tests
 */

const getPastDate = (daysAgo: number): string => {
  const date = new Date();

  date.setDate(date.getDate() - daysAgo);

  return date.toISOString().slice(0, 16);
};

// Run backfill tests serially to avoid conflicts with other tests using the same DAG
test.describe.serial("Backfill Tabs", () => {
  let loginPage: LoginPage;
  let backfillPage: BackfillPage;

  const testCredentials = testConfig.credentials;
  const testDagId = testConfig.testDag.id;

  test.beforeEach(async ({ page }) => {
    loginPage = new LoginPage(page);
    backfillPage = new BackfillPage(page);
    await loginPage.navigateAndLogin(testCredentials.username, testCredentials.password);
    await loginPage.expectLoginSuccess();
  });

  test("verify date range selection (start date, end date)", async () => {
    // Set start date after end date to trigger validation error
    const fromDate = getPastDate(1);
    const toDate = getPastDate(7);

    await backfillPage.navigateToDagDetail(testDagId);
    await backfillPage.openBackfillDialog();

    await backfillPage.backfillFromDateInput.fill(fromDate);
    await backfillPage.backfillToDateInput.fill(toDate);

    await backfillPage.page.waitForTimeout(1000);

    const isErrorVisible = await backfillPage.isBackfillDateErrorVisible();

    expect(isErrorVisible).toBe(true);
  });

  test("should create backfill with 'All Runs' behavior", async () => {
    const fromDate = getPastDate(2);
    const toDate = getPastDate(1);
    const expectedFromDate = fromDate.replace("T", " ");
    const expectedToDate = toDate.replace("T", " ");

    await backfillPage.createBackfill(testDagId, { fromDate, reprocessBehavior: "All Runs", toDate });

    // Verify backfill status is visible (indicates a backfill is running/queued)
    const backfillStatus = backfillPage.page.locator('[data-testid="backfill-status"]');

    await expect(backfillStatus).toBeVisible();

    // Navigate to backfills tab and verify our specific backfill appears in the list
    await backfillPage.navigateToBackfillsTab(testDagId);
    await backfillPage.waitForPageLoad();

    const backfillRows = await backfillPage.getBackfillsTableRows();

    expect(backfillRows).toBeGreaterThanOrEqual(1);

    // Verify our specific backfill exists (matching from date, to date, and reprocess behavior)
    const backfillRow = backfillPage.page.locator(
      `table tbody tr:has(td:has-text("${expectedFromDate}")):has(td:has-text("${expectedToDate}")):has(td:has-text("All Runs"))`,
    );

    await expect(backfillRow.first()).toBeVisible();
  });
});
