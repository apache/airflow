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
import { testConfig, AUTH_FILE } from "playwright.config";
import { BackfillPage } from "tests/e2e/pages/BackfillPage";

const getPastDate = (daysAgo: number): string => {
  const date = new Date();

  date.setDate(date.getDate() - daysAgo);
  date.setHours(0, 0, 0, 0);

  return date.toISOString().slice(0, 16);
};

test.describe("Backfill creation and validation", () => {
  // Serial mode ensures all tests run on one worker, preventing parallel beforeAll conflicts
  test.describe.configure({ mode: "serial" });
  test.setTimeout(240_000);

  const testDagId = testConfig.testDag.id;

  const backfillConfigs = [
    { behavior: "All Runs" as const, fromDate: getPastDate(5), toDate: getPastDate(4) },
    { behavior: "Missing Runs" as const, fromDate: getPastDate(8), toDate: getPastDate(6) },
    { behavior: "Missing and Errored Runs" as const, fromDate: getPastDate(12), toDate: getPastDate(10) },
  ];

  test.beforeAll(async ({ browser }) => {
    test.setTimeout(600_000);

    const context = await browser.newContext({ storageState: AUTH_FILE });
    const page = await context.newPage();
    const setupBackfillPage = new BackfillPage(page);

    for (const config of backfillConfigs) {
      await setupBackfillPage.createBackfill(testDagId, {
        fromDate: config.fromDate,
        reprocessBehavior: config.behavior,
        toDate: config.toDate,
      });

      // Wait for backfill to complete on Dag detail page (where the banner is visible)
      await setupBackfillPage.navigateToDagDetail(testDagId);
      await setupBackfillPage.waitForNoActiveBackfill();
    }

    await context.close();
  });

  test("verify backfill with 'all runs' behavior", async ({ page }) => {
    const backfillPage = new BackfillPage(page);

    await backfillPage.navigateToBackfillsTab(testDagId);

    const config = backfillConfigs[0]!; // All Runs

    const backfillDetails = await backfillPage.getBackfillDetailsByDateRange({
      fromDate: config.fromDate,
      toDate: config.toDate,
    });

    expect(backfillDetails.fromDate.slice(0, 10)).toEqual(config.fromDate.slice(0, 10));
    expect(backfillDetails.toDate.slice(0, 10)).toEqual(config.toDate.slice(0, 10));

    expect(backfillDetails.createdAt).not.toEqual("");
    expect(backfillDetails.reprocessBehavior).toEqual("All Runs");
    const status = await backfillPage.getBackfillStatus();

    expect(status).not.toEqual("");
  });

  test("verify backfill with 'missing runs' behavior", async ({ page }) => {
    const backfillPage = new BackfillPage(page);

    await backfillPage.navigateToBackfillsTab(testDagId);

    const config = backfillConfigs[1]!;

    const backfillDetails = await backfillPage.getBackfillDetailsByDateRange({
      fromDate: config.fromDate,
      toDate: config.toDate,
    });

    expect(backfillDetails.fromDate.slice(0, 10)).toEqual(config.fromDate.slice(0, 10));
    expect(backfillDetails.toDate.slice(0, 10)).toEqual(config.toDate.slice(0, 10));

    expect(backfillDetails.createdAt).not.toEqual("");
    expect(backfillDetails.reprocessBehavior).toEqual("Missing Runs");
    const status = await backfillPage.getBackfillStatus();

    expect(status).not.toEqual("");
  });

  test("verify backfill with 'missing and errored runs' behavior", async ({ page }) => {
    const backfillPage = new BackfillPage(page);

    await backfillPage.navigateToBackfillsTab(testDagId);

    const config = backfillConfigs[2]!;

    const backfillDetails = await backfillPage.getBackfillDetailsByDateRange({
      fromDate: config.fromDate,
      toDate: config.toDate,
    });

    expect(backfillDetails.fromDate.slice(0, 10)).toEqual(config.fromDate.slice(0, 10));
    expect(backfillDetails.toDate.slice(0, 10)).toEqual(config.toDate.slice(0, 10));

    expect(backfillDetails.createdAt).not.toEqual("");
    expect(backfillDetails.reprocessBehavior).toEqual("Missing and Errored Runs");
    const status = await backfillPage.getBackfillStatus();

    expect(status).not.toEqual("");
  });

  test("Verify backfill table filters", async ({ page }) => {
    const backfillPage = new BackfillPage(page);

    await backfillPage.navigateToBackfillsTab(testDagId);

    const initialColumnCount = await backfillPage.getTableColumnCount();

    expect(initialColumnCount).toBeGreaterThan(0);
    await expect(backfillPage.getFilterButton()).toBeVisible();

    await backfillPage.openFilterMenu();

    const filterMenuItems = backfillPage.page.locator('[role="menuitem"]');
    const filterMenuCount = await filterMenuItems.count();

    expect(filterMenuCount).toBeGreaterThan(0);

    const firstMenuItem = filterMenuItems.first();
    const columnToToggle = (await firstMenuItem.textContent())?.trim() ?? "";

    expect(columnToToggle).not.toBe("");

    await backfillPage.toggleColumn(columnToToggle);
    await backfillPage.backfillsTable.click({ position: { x: 5, y: 5 } });

    await expect(backfillPage.getColumnHeader(columnToToggle)).not.toBeVisible();

    const newColumnCount = await backfillPage.getTableColumnCount();

    expect(newColumnCount).toBeLessThan(initialColumnCount);

    await backfillPage.openFilterMenu();
    await backfillPage.toggleColumn(columnToToggle);
    await backfillPage.backfillsTable.click({ position: { x: 5, y: 5 } });

    await expect(backfillPage.getColumnHeader(columnToToggle)).toBeVisible();

    const finalColumnCount = await backfillPage.getTableColumnCount();

    expect(finalColumnCount).toBe(initialColumnCount);
  });
});

test.describe("validate date range", () => {
  test.setTimeout(60_000);

  const testDagId = testConfig.testDag.id;

  test("verify date range selection (start date, end date)", async ({ page }) => {
    const fromDate = getPastDate(1);
    const toDate = getPastDate(7);
    const backfillPage = new BackfillPage(page);

    await backfillPage.navigateToDagDetail(testDagId);
    await backfillPage.openBackfillDialog();
    await backfillPage.backfillFromDateInput.fill(fromDate);
    await backfillPage.backfillToDateInput.fill(toDate);
    await expect(backfillPage.backfillDateError).toBeVisible();
  });
});

test.describe("Backfill pause, resume, and cancel controls", () => {
  test.describe.configure({ mode: "serial" });
  test.setTimeout(180_000);

  const testDagId = testConfig.testDag.id;
  const controlFromDate = getPastDate(90);
  const controlToDate = getPastDate(30);

  let backfillPage: BackfillPage;

  test.beforeEach(async ({ page }) => {
    backfillPage = new BackfillPage(page);
    await backfillPage.navigateToDagDetail(testDagId);

    if (await backfillPage.cancelButton.isVisible({ timeout: 5000 }).catch(() => false)) {
      await backfillPage.clickCancelButton();
    }

    await backfillPage.createBackfill(testDagId, {
      fromDate: controlFromDate,
      reprocessBehavior: "All Runs",
      toDate: controlToDate,
    });

    await expect(async () => {
      await backfillPage.page.reload();
      await expect(backfillPage.triggerButton).toBeVisible({ timeout: 10_000 });
      await expect(backfillPage.pauseButton).toBeVisible({ timeout: 5000 });
    }).toPass({ timeout: 60_000 });
  });

  test.afterEach(async () => {
    await backfillPage.navigateToDagDetail(testDagId);
    if (await backfillPage.cancelButton.isVisible({ timeout: 5000 }).catch(() => false)) {
      await backfillPage.clickCancelButton();
    }
  });

  test("verify pause and resume backfill", async () => {
    await backfillPage.clickPauseButton();
    expect(await backfillPage.isBackfillPaused()).toBe(true);

    await backfillPage.clickPauseButton();
    expect(await backfillPage.isBackfillPaused()).toBe(false);
  });

  test("verify cancel backfill", async () => {
    await backfillPage.clickCancelButton();
    await expect(backfillPage.pauseButton).not.toBeVisible({ timeout: 10_000 });
    await expect(backfillPage.cancelButton).not.toBeVisible({ timeout: 10_000 });
  });

  test("verify cancelled backfill cannot be resumed", async () => {
    await backfillPage.clickCancelButton();
    await expect(backfillPage.pauseButton).not.toBeVisible({ timeout: 10_000 });

    await backfillPage.page.reload();
    await expect(backfillPage.triggerButton).toBeVisible({ timeout: 30_000 });
    await expect(backfillPage.pauseButton).not.toBeVisible({ timeout: 10_000 });

    await backfillPage.navigateToBackfillsTab(testDagId);

    const row = await backfillPage.findBackfillRowByDateRange({
      fromDate: controlFromDate,
      toDate: controlToDate,
    });

    await expect(row).toBeVisible();

    const columnMap = await backfillPage.getColumnIndexMap();
    const completedAtIndex = BackfillPage.findColumnIndex(columnMap, ["Completed at", "table.completedAt"]);
    const completedAtCell = row.locator("td").nth(completedAtIndex);
    const completedAtText = await completedAtCell.textContent();

    expect(completedAtText?.trim()).not.toBe("");
  });
});
