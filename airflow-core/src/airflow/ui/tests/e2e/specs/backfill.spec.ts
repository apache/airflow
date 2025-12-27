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

  return date.toISOString().slice(0, 16);
};

// Backfills E2E Tests

test.describe("Backfills List Display", () => {
  let backfillPage: BackfillPage;

  const testDagId = testConfig.testDag.id;

  test.beforeEach(async ({ page }) => {
    backfillPage = new BackfillPage(page);

    const fromDate = getPastDate(2);
    const toDate = getPastDate(1);

    await backfillPage.createBackfill(testDagId, {
      fromDate,
      reprocessBehavior: "All Runs",
      toDate,
    });

    await backfillPage.navigateToBackfillsTab(testDagId);
  });

  test("should Verify backfills list displays (or empty state if none)", async () => {
    const rowsCount = await backfillPage.getBackfillsTableRows();

    if (rowsCount > 0) {
      await expect(backfillPage.backfillsTable).toBeVisible();
      expect(rowsCount).toBeGreaterThanOrEqual(1);
    } else {
      await expect(backfillPage.page.locator('text="No backfills found"')).toBeVisible();
    }
  });

  test("Verify backfill details display: date range, status, created time", async () => {
    const backfillDetails = await backfillPage.getBackfillDetails(0);

    expect(backfillDetails.fromDate).toBeTruthy();
    expect(backfillDetails.toDate).toBeTruthy();

    expect(backfillDetails.reprocessBehavior).toBeTruthy();
    expect(["All Runs", "Missing Runs", "Missing and Errored Runs"]).toContain(
      backfillDetails.reprocessBehavior,
    );

    expect(backfillDetails.createdAt).toBeTruthy();
  });

  test("should verify Table filters", async () => {
    const initialColumnCount = await backfillPage.getTableColumnCount();

    expect(initialColumnCount).toBeGreaterThan(0);

    const isFilterAvailable = await backfillPage.isFilterAvailable();

    expect(isFilterAvailable).toBe(true);

    await backfillPage.openFilterMenu();

    const columnToToggle = "Duration";
    const isInitiallyVisible = await backfillPage.isColumnVisible(columnToToggle);

    if (isInitiallyVisible) {
      await backfillPage.toggleColumn(columnToToggle);

      await backfillPage.page.keyboard.press("Escape");
      await backfillPage.page.waitForTimeout(500);

      const isVisibleAfterToggle = await backfillPage.isColumnVisible(columnToToggle);

      expect(isVisibleAfterToggle).toBe(false);

      const newColumnCount = await backfillPage.getTableColumnCount();

      expect(newColumnCount).toBeLessThan(initialColumnCount);

      await backfillPage.openFilterMenu();
      await backfillPage.toggleColumn(columnToToggle);
      await backfillPage.page.keyboard.press("Escape");
      await backfillPage.page.waitForTimeout(500);

      const isVisibleAfterRestore = await backfillPage.isColumnVisible(columnToToggle);

      expect(isVisibleAfterRestore).toBe(true);

      const finalColumnCount = await backfillPage.getTableColumnCount();

      expect(finalColumnCount).toBe(initialColumnCount);
    }
  });
});
