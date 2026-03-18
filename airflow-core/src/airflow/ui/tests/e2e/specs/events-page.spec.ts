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

  test("verify events page displays correctly", async () => {
    await eventsPage.navigate();

    await expect(eventsPage.eventsPageTitle).toBeVisible({ timeout: 10_000 });
    await expect(eventsPage.eventsTable).toBeVisible();
    await eventsPage.verifyTableColumns();
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
});

test.describe("Events with Generated Data", () => {
  let eventsPage: EventsPage;
  const testDagId = testConfig.testDag.id;

  test.setTimeout(60_000);

  test.beforeAll(async ({ browser }) => {
    test.setTimeout(3 * 60 * 1000);
    const context = await browser.newContext({ storageState: AUTH_FILE });
    const page = await context.newPage();
    const dagsPage = new DagsPage(page);

    await dagsPage.triggerDag(testDagId);
    await context.close();
  });

  test.beforeEach(({ page }) => {
    eventsPage = new EventsPage(page);
  });

  test("verify audit log entries display valid data", async () => {
    await eventsPage.navigate();

    await expect(eventsPage.eventsTable).toBeVisible();

    const rowCount = await eventsPage.getTableRowCount();

    expect(rowCount).toBeGreaterThan(0);
    await eventsPage.verifyLogEntriesWithData();
  });

  test("verify search for specific event type and filtered results", async () => {
    await eventsPage.navigate();

    const initialRowCount = await eventsPage.getTableRowCount();

    expect(initialRowCount).toBeGreaterThan(0);

    await eventsPage.addFilter("Event Type");
    await eventsPage.setFilterValue("Event Type", "cli");
    await expect(eventsPage.eventsTable).toBeVisible();

    await expect(async () => {
      const filteredEvents = await eventsPage.getEventTypes();

      expect(filteredEvents.length).toBeGreaterThan(0);
      for (const event of filteredEvents) {
        expect(event.toLowerCase()).toContain("cli");
      }
    }).toPass({ timeout: 20_000 });
  });

  test("verify filter by DAG ID", async () => {
    await eventsPage.navigate();
    await eventsPage.addFilter("DAG ID");
    await eventsPage.setFilterValue("DAG ID", testDagId);
    await expect(eventsPage.eventsTable).toBeVisible();

    await expect(async () => {
      const rows = await eventsPage.getEventLogRows();

      expect(rows.length).toBeGreaterThan(0);

      await expect(eventsPage.eventsTable).toBeVisible();

      for (const row of rows) {
        const dagIdCell = await eventsPage.getCellByColumnName(row, "DAG ID");
        const dagIdText = await dagIdCell.textContent();

        expect(dagIdText?.toLowerCase()).toContain(testDagId.toLowerCase());
      }
    }).toPass({ timeout: 20_000 });
  });
});
