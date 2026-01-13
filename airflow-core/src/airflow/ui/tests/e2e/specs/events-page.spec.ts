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
import { DagsPage } from "tests/e2e/pages/DagsPage";
import { EventsPage } from "tests/e2e/pages/EventsPage";

test.describe("Events Page", () => {
  let eventsPage: EventsPage;

  test.beforeEach(({ page }) => {
    eventsPage = new EventsPage(page);
  });

  test("verify search input is visible", async () => {
    await eventsPage.navigate();
    await eventsPage.waitForEventsTable();

    // Verify filter bar (containing search functionality) is visible
    await expect(eventsPage.filterBar).toBeVisible({ timeout: 10_000 });

    // Verify the filter button is present (allows adding search filters)
    const filterButton = eventsPage.page.locator('button:has-text("Filter")');

    await expect(filterButton).toBeVisible();

    // Click the filter button to open the filter menu
    await filterButton.click();

    // Verify filter menu opened - be more specific to target the filter menu
    const filterMenu = eventsPage.page.locator('[role="menu"][aria-labelledby*="menu"][data-state="open"]');

    await expect(filterMenu).toBeVisible({ timeout: 5000 });

    // Look for text search options in the menu
    const textSearchOptions = eventsPage.page.locator(
      '[role="menuitem"]:has-text("DAG ID"), [role="menuitem"]:has-text("Event Type"), [role="menuitem"]:has-text("User")',
    );

    const textSearchOptionsCount = await textSearchOptions.count();

    expect(textSearchOptionsCount).toBeGreaterThan(0);
    await expect(textSearchOptions.first()).toBeVisible();

    // Close the menu by pressing Escape
    await eventsPage.page.keyboard.press("Escape");
  });

  test("verify events page", async () => {
    await eventsPage.navigate();

    await expect(async () => {
      // To avoid flakiness, we use a promise to wait for the elements to be visible
      await expect(eventsPage.eventsPageTitle).toBeVisible();
      await eventsPage.waitForEventsTable();
      await expect(eventsPage.eventsTable).toBeVisible();
      await eventsPage.verifyTableColumns();
    }).toPass({ timeout: 30_000 });
  });
});

test.describe("Events with Generated Data", () => {
  let eventsPage: EventsPage;
  const testDagId = testConfig.testDag.id;

  test.beforeAll(async ({ browser }) => {
    test.setTimeout(5 * 60 * 1000); // 5 minutes timeout

    // First, trigger a DAG to generate audit log entries
    const context = await browser.newContext({ storageState: AUTH_FILE });
    const page = await context.newPage();
    const dagsPage = new DagsPage(page);

    await dagsPage.triggerDag(testDagId);
    await context.close();
  });

  test.beforeEach(({ page }) => {
    eventsPage = new EventsPage(page);
  });

  test("verify audit log entries are shown correctly", async () => {
    await eventsPage.navigate();

    await expect(async () => {
      // Wait for table to load
      await eventsPage.waitForEventsTable();
      // Verify the log entries contain actual data
      await eventsPage.verifyLogEntriesWithData();
    }).toPass({ timeout: 30_000 });
  });

  test("verify pagination works with small page size", async () => {
    // Navigate to events page with small page size to force pagination
    await eventsPage.navigateToPaginatedEventsPage();

    await eventsPage.waitForEventsTable();

    await expect(eventsPage.paginationNextButton).toBeVisible({ timeout: 10_000 });
    await expect(eventsPage.paginationPrevButton).toBeVisible({ timeout: 10_000 });

    // Test pagination functionality - should have enough data to enable next button
    await expect(eventsPage.paginationNextButton).toBeEnabled({ timeout: 10_000 });

    // Click next page - content should change
    await eventsPage.clickNextPage();
    await expect(eventsPage.eventsTable).toBeVisible({ timeout: 10_000 });

    //  Click prev page - content should change and previous button should be enabled₹
    await expect(eventsPage.paginationPrevButton).toBeEnabled({ timeout: 5000 });

    await eventsPage.clickPrevPage();
    await expect(eventsPage.eventsTable).toBeVisible({ timeout: 10_000 });
  });

  test("verify column sorting works", async () => {
    await eventsPage.navigate();
    await expect(async () => {
      await eventsPage.waitForEventsTable();
      await eventsPage.verifyLogEntriesWithData();
    }).toPass({ timeout: 20_000 });

    // Get initial timestamps before sorting (first 3 rows)
    const initialTimestamps = [];
    const rowCount = await eventsPage.getTableRowCount();

    for (let i = 0; i < rowCount; i++) {
      const timestamp = await eventsPage.getCellContent(i, 0);

      initialTimestamps.push(timestamp);
    }

    // Click timestamp column header until we get ascending sort.
    let maxIterations = 5;

    while (maxIterations > 0) {
      await expect(eventsPage.eventsTable).toBeVisible({ timeout: 5000 });
      const sortIndicator = await eventsPage.getColumnSortIndicator("when");

      if (sortIndicator !== "none") {
        break;
      }
      await eventsPage.clickColumnHeader("when");
      maxIterations--;
    }

    await expect(async () => {
      await eventsPage.waitForEventsTable();
      await eventsPage.verifyLogEntriesWithData();
    }).toPass({ timeout: 20_000 });

    // Get timestamps after sorting
    const sortedTimestamps = [];

    for (let i = 0; i < rowCount; i++) {
      const timestamp = await eventsPage.getCellContent(i, 0);

      sortedTimestamps.push(timestamp);
    }

    let initialSortedTimestamps = [...initialTimestamps].sort(
      (a, b) => new Date(a).getTime() - new Date(b).getTime(),
    );
    // Verify that the order matches either ascending or descending sorted timestamps
    const initialSortedDescending = [...initialSortedTimestamps].reverse();
    const isAscending = sortedTimestamps.every(
      (timestamp, index) => timestamp === initialSortedTimestamps[index],
    );
    const isDescending = sortedTimestamps.every(
      (timestamp, index) => timestamp === initialSortedDescending[index],
    );

    expect(isAscending || isDescending).toBe(true);
  });
});
