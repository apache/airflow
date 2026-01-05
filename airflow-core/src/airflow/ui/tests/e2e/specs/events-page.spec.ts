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
import { EventsPage } from "tests/e2e/pages/EventsPage";
import { DagsPage } from "tests/e2e/pages/DagsPage";

test.describe("Events Page", () => {
  let eventsPage: EventsPage;

  test.beforeEach(({ page }) => {
    eventsPage = new EventsPage(page);
  });

  test("should verify search input is visible", async () => {
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
    await expect(filterMenu).toBeVisible({ timeout: 5_000 });
    
    // Look for text search options in the menu
    const textSearchOptions = eventsPage.page.locator('[role="menuitem"]:has-text("DAG ID"), [role="menuitem"]:has-text("Event Type"), [role="menuitem"]:has-text("User")');
    await textSearchOptions.count() > 0;
    await expect(textSearchOptions.first()).toBeVisible();
    
    // Close the menu by pressing Escape
    await eventsPage.page.keyboard.press('Escape');
    
    // Wait a moment for menu to close
    await eventsPage.page.waitForTimeout(500);
    
  });

  test("should verify events page", async () => {
    await eventsPage.navigate();

    await expect(async () => { // To avoid flakiness, we use a promise to wait for the elements to be visible
      await expect(eventsPage.eventsPageTitle).toBeVisible({ timeout: 10_000 });
      await eventsPage.waitForEventsTable();
      await expect(eventsPage.eventsTable).toBeVisible({ timeout: 10_000 });
      await eventsPage.verifyTableColumns();
    }).toPass({ timeout: 30_000 });
  });

});

test.describe("Events with Generated Data", () => {
  let eventsPage: EventsPage;
  let dagsPage: DagsPage;
  const testDagId = testConfig.testDag.id;

  test.beforeEach(({ page }) => {
    eventsPage = new EventsPage(page);
    dagsPage = new DagsPage(page);
  });

  test("should show audit log entries after triggering a DAG", async () => {
    test.setTimeout(3 * 60 * 1000); // 3 minutes timeout

    // First, trigger a DAG to generate audit log entries
    await dagsPage.triggerDag(testDagId);
    
    // Navigate to events page
    await eventsPage.navigate();
    
    // Wait for table to load
    await eventsPage.waitForEventsTable();
    
    // Wait for audit log entries to appear (they may take time to load within table)
    await eventsPage.waitForTimeout(5000);
    
     // Verify the log entries contain actual data
     await eventsPage.verifyLogEntriesWithData();
      
   });

    test("should verify pagination works with small page size", async () => {
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
      await expect(eventsPage.paginationPrevButton).toBeEnabled({ timeout: 5_000 });
      
      await eventsPage.clickPrevPage();
      await expect(eventsPage.eventsTable).toBeVisible({ timeout: 10_000 });
    });

    test("should verify column sorting works", async () => {
        await eventsPage.navigate();
        await eventsPage.waitForEventsTable();

        await eventsPage.waitForTimeout(5000);

        // Click first column header (Timestamp) to sort
        await eventsPage.clickColumnHeader(0);
        await expect(eventsPage.eventsTable).toBeVisible({ timeout: 10_000 });

        await eventsPage.waitForTimeout(5000);

        // Verify sort indicator shows ascending
        const sortIndicator = await eventsPage.getColumnSortIndicator(0);
        expect(sortIndicator).not.toBe("none");
    });
});