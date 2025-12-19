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
import { DagsPage } from "tests/e2e/pages/DagsPage";
import { LoginPage } from "tests/e2e/pages/LoginPage";

/**
 * Backfill E2E Tests
 *
 * Tests for creating backfills with different reprocess behaviors
 * as specified in issue #59593
 */

// Helper to generate past dates for backfill
const getPastDate = (daysAgo: number): string => {
  const date = new Date();

  date.setDate(date.getDate() - daysAgo);

  return date.toISOString().slice(0, 16); // Format: YYYY-MM-DDTHH:mm
};

test.describe("Backfill Creation", () => {
  let loginPage: LoginPage;
  let dagsPage: DagsPage;

  const testCredentials = testConfig.credentials;
  const testDagId = testConfig.testDag.id;

  test.beforeEach(async ({ page }) => {
    loginPage = new LoginPage(page);
    dagsPage = new DagsPage(page);
    await loginPage.navigateAndLogin(testCredentials.username, testCredentials.password);
    await loginPage.expectLoginSuccess();
  });

  test("should navigate to DAG Backfills tab", async () => {
    await dagsPage.navigateToBackfillsTab(testDagId);

    // Verify we're on the backfills page by checking the URL
    const currentUrl = dagsPage.page.url();

    expect(currentUrl).toContain(`/dags/${testDagId}/backfills`);
  });

  test("should open backfill dialog from DAG detail page", async () => {
    await dagsPage.navigateToDagDetail(testDagId);
    await dagsPage.openBackfillDialog();

    // Verify backfill form elements are visible
    await expect(dagsPage.backfillFromDateInput).toBeVisible();
    await expect(dagsPage.backfillToDateInput).toBeVisible();
    await expect(dagsPage.backfillRunButton).toBeVisible();
  });

  test("should create backfill with 'reprocess none' (Missing Runs) behavior", async () => {
    test.setTimeout(3 * 60 * 1000);

    const fromDate = getPastDate(7);
    const toDate = getPastDate(1);

    await dagsPage.createBackfill(testDagId, { fromDate, reprocessBehavior: "none", toDate });

    // Navigate to backfills tab to verify creation
    await dagsPage.navigateToBackfillsTab(testDagId);
    await dagsPage.waitForPageLoad();

    // Verify backfill appears in the list
    const backfillRows = await dagsPage.getBackfillsTableRows();

    expect(backfillRows).toBeGreaterThanOrEqual(1);
  });

  test("should create backfill with 'reprocess failed' (Missing and Errored Runs) behavior", async () => {
    test.setTimeout(3 * 60 * 1000);

    const fromDate = getPastDate(14);
    const toDate = getPastDate(8);

    await dagsPage.createBackfill(testDagId, { fromDate, reprocessBehavior: "failed", toDate });

    // Navigate to backfills tab to verify creation
    await dagsPage.navigateToBackfillsTab(testDagId);
    await dagsPage.waitForPageLoad();

    // Verify backfill appears in the list
    const backfillRows = await dagsPage.getBackfillsTableRows();

    expect(backfillRows).toBeGreaterThanOrEqual(1);
  });

  test("should create backfill with 'reprocess completed' (All Runs) behavior", async () => {
    test.setTimeout(3 * 60 * 1000);

    const fromDate = getPastDate(21);
    const toDate = getPastDate(15);

    await dagsPage.createBackfill(testDagId, { fromDate, reprocessBehavior: "completed", toDate });

    // Navigate to backfills tab to verify creation
    await dagsPage.navigateToBackfillsTab(testDagId);
    await dagsPage.waitForPageLoad();

    // Verify backfill appears in the list
    const backfillRows = await dagsPage.getBackfillsTableRows();

    expect(backfillRows).toBeGreaterThanOrEqual(1);
  });

  test("should show validation error for invalid date range", async () => {
    // Set start date after end date to trigger validation error
    const fromDate = getPastDate(1); // Yesterday
    const toDate = getPastDate(7); // 7 days ago (before start date)

    await dagsPage.navigateToDagDetail(testDagId);
    await dagsPage.openBackfillDialog();

    // Fill invalid date range (start date > end date)
    await dagsPage.backfillFromDateInput.fill(fromDate);
    await dagsPage.backfillToDateInput.fill(toDate);

    // Wait for validation to trigger
    await dagsPage.page.waitForTimeout(1000);

    // Check that the error message is visible
    const isErrorVisible = await dagsPage.isBackfillDateErrorVisible();

    expect(isErrorVisible).toBe(true);
  });

  test("should verify backfills list shows reprocess behavior column", async () => {
    await dagsPage.navigateToBackfillsTab(testDagId);
    await dagsPage.waitForPageLoad();

    // Check that the table header contains "Reprocess Behavior"
    const reprocessHeader = dagsPage.page.locator('th:has-text("Reprocess Behavior")');

    await expect(reprocessHeader).toBeVisible();
  });
});
