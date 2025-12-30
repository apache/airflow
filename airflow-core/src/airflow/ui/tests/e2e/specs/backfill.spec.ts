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
import { BackfillPage } from "tests/e2e/pages/BackfillPage";

const getPastDate = (daysAgo: number): string => {
  const date = new Date();

  date.setDate(date.getDate() - daysAgo);
  date.setHours(0, 0, 0, 0);

  return date.toISOString().slice(0, 16);
};

// Backfills E2E Tests

test.describe("Backfill creation", () => {
  let backfillPage: BackfillPage;
  const testDagId = testConfig.testDag.id;

  test.beforeEach(({ page }) => {
    test.setTimeout(90_000);
    backfillPage = new BackfillPage(page);
  });

  test("verify date range selection (start date, end date)", async () => {
    const fromDate = getPastDate(1);
    const toDate = getPastDate(7);

    await backfillPage.navigateToDagDetail(testDagId);
    await backfillPage.openBackfillDialog();
    await backfillPage.backfillFromDateInput.fill(fromDate);
    await backfillPage.backfillToDateInput.fill(toDate);
    await expect(backfillPage.backfillDateError).toBeVisible();
  });

  test("Should create backfill with 'all runs' behavior", async () => {
    const createdFromDate = getPastDate(10);
    const createdToDate = getPastDate(5);

    await backfillPage.createBackfill(testDagId, {
      fromDate: createdFromDate,
      reprocessBehavior: "All Runs",
      toDate: createdToDate,
    });

    await backfillPage.navigateToBackfillsTab(testDagId);
  });

  test("Should create backfill with 'Missing Runs' behavior", async () => {
    const createdFromDate = getPastDate(45);
    const createdToDate = getPastDate(30);

    await backfillPage.createBackfill(testDagId, {
      fromDate: createdFromDate,
      reprocessBehavior: "Missing Runs",
      toDate: createdToDate,
    });

    await backfillPage.navigateToBackfillsTab(testDagId);
  });

  test("Should create backfill with 'Missing and Errored runs' behavior", async () => {
    const createdFromDate = getPastDate(60);
    const createdToDate = getPastDate(55);

    await backfillPage.createBackfill(testDagId, {
      fromDate: createdFromDate,
      reprocessBehavior: "Missing and Errored Runs",
      toDate: createdToDate,
    });

    await backfillPage.navigateToBackfillsTab(testDagId);
  });
});

test.describe("Backfills List Display", () => {
  let backfillPage: BackfillPage;
  const testDagId = testConfig.testDag.id;
  let createdFromDate: string;
  let createdToDate: string;

  test.beforeAll(async ({ browser }) => {
    test.setTimeout(60_000);

    const context = await browser.newContext();
    const page = await context.newPage();

    backfillPage = new BackfillPage(page);

    createdFromDate = getPastDate(2);
    createdToDate = getPastDate(1);

    await backfillPage.createBackfill(testDagId, {
      fromDate: createdFromDate,
      reprocessBehavior: "All Runs",
      toDate: createdToDate,
    });

    await backfillPage.navigateToBackfillsTab(testDagId);
  });

  test("should verify backfills list display", async () => {
    await expect(backfillPage.backfillsTable).toBeVisible();

    const rowIndex = await backfillPage.findBackfillByDateRange(createdFromDate, createdToDate);

    expect(rowIndex).not.toBeNull();

    const foundRow = backfillPage.page.locator("table tbody tr").nth(rowIndex!);

    await expect(foundRow).toBeVisible();
  });

  test("Verify backfill details display: date range, status, created time", async () => {
    const backfillDetails = await backfillPage.getBackfillDetails(createdFromDate, createdToDate);

    expect(backfillDetails.fromDate.slice(0, 10)).toEqual(createdFromDate.slice(0, 10));
    expect(backfillDetails.toDate.slice(0, 10)).toEqual(createdToDate.slice(0, 10));

    const status = await backfillPage.getBackfillStatus();

    expect(status).not.toEqual("");

    expect(backfillDetails.createdAt).not.toEqual("");
  });

  test("should verify Table filters", async () => {
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

    await backfillPage.page.keyboard.press("Escape");
    await backfillPage.page.locator('[role="menu"]').waitFor({ state: "hidden" });

    await expect(backfillPage.getColumnHeader(columnToToggle)).not.toBeVisible();

    const newColumnCount = await backfillPage.getTableColumnCount();

    expect(newColumnCount).toBeLessThan(initialColumnCount);

    await backfillPage.openFilterMenu();
    await backfillPage.toggleColumn(columnToToggle);

    await backfillPage.page.keyboard.press("Escape");
    await backfillPage.page.locator('[role="menu"]').waitFor({ state: "hidden" });

    await expect(backfillPage.getColumnHeader(columnToToggle)).toBeVisible();

    const finalColumnCount = await backfillPage.getTableColumnCount();

    expect(finalColumnCount).toBe(initialColumnCount);
  });
});
