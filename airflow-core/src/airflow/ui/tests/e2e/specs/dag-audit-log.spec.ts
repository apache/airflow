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
import { DagsPage } from "tests/e2e/pages/DagsPage";
import { EventsPage } from "tests/e2e/pages/EventsPage";

test.describe("DAG Audit Log", () => {
  let eventsPage: EventsPage;

  const testDagId = testConfig.testDag.id;
  const triggerCount = 3;
  const expectedEventCount = triggerCount + 1;

  test.setTimeout(60_000);

  test.beforeAll(async ({ browser }) => {
    test.setTimeout(3 * 60 * 1000);
    const context = await browser.newContext({ storageState: AUTH_FILE });
    const page = await context.newPage();
    const setupDagsPage = new DagsPage(page);
    const setupEventsPage = new EventsPage(page);

    for (let i = 0; i < triggerCount; i++) {
      await setupDagsPage.triggerDag(testDagId);
    }

    await setupEventsPage.navigateToAuditLog(testDagId);
    await page.waitForFunction(
      (minCount) => {
        const table = document.querySelector('[data-testid="table-list"]');

        if (!table) {
          return false;
        }
        const rows = table.querySelectorAll("tbody tr");

        return rows.length >= minCount;
      },
      expectedEventCount,
      { timeout: 60_000 },
    );

    await context.close();
  });

  test.beforeEach(({ page }) => {
    eventsPage = new EventsPage(page);
  });

  test("should navigate to audit log tab and display table", async () => {
    await eventsPage.navigateToAuditLog(testDagId);

    await expect(eventsPage.eventsTable).toBeVisible();

    const rowCount = await eventsPage.tableRows.count();

    expect(rowCount).toBeGreaterThan(0);
  });

  test("should display all expected columns in audit log table", async () => {
    await eventsPage.navigateToAuditLog(testDagId);

    await expect(eventsPage.whenColumn).toBeVisible();
    await expect(eventsPage.eventColumn).toBeVisible();
    await expect(eventsPage.ownerColumn).toBeVisible();
    await expect(eventsPage.extraColumn).toBeVisible();

    const dagIdColumn = eventsPage.eventsTable.locator('th:has-text("DAG ID")');

    await expect(dagIdColumn).not.toBeVisible();
  });

  test("should display audit log entries with valid data", async () => {
    await eventsPage.navigateToAuditLog(testDagId);

    const rows = await eventsPage.getEventLogRows();

    expect(rows.length).toBeGreaterThan(0);

    const [firstRow] = rows;

    if (!firstRow) {
      throw new Error("No rows found");
    }

    const cells = firstRow.locator("td");

    const whenCell = cells.nth(0);
    const eventCell = cells.nth(1);
    const userCell = cells.nth(2);

    const whenText = await whenCell.textContent();
    const eventText = await eventCell.textContent();
    const userText = await userCell.textContent();

    expect(whenText).toBeTruthy();
    expect(whenText?.trim()).not.toBe("");

    expect(eventText).toBeTruthy();
    expect(eventText?.trim()).not.toBe("");

    expect(userText).toBeTruthy();
    expect(userText?.trim()).not.toBe("");
  });

  test("should paginate through audit log entries", async () => {
    await eventsPage.navigateToAuditLog(testDagId, 3);

    const hasNext = await eventsPage.hasNextPage();

    expect(hasNext).toBe(true);

    const [firstRowPage1] = await eventsPage.getEventLogRows();
    const runIdPage1 = await firstRowPage1?.locator("td").nth(4).textContent();

    await eventsPage.clickNextPage();

    const [firstRowPage2] = await eventsPage.getEventLogRows();
    const runIdPage2 = await firstRowPage2?.locator("td").nth(4).textContent();

    expect(runIdPage2).not.toBe(runIdPage1);

    await eventsPage.clickPrevPage();

    const [firstRowBackToPage1] = await eventsPage.getEventLogRows();
    const runIdBackToPage1 = await firstRowBackToPage1?.locator("td").nth(4).textContent();

    expect(runIdBackToPage1).toBe(runIdPage1);
  });

  test("should sort audit log entries when clicking column header", async () => {
    await eventsPage.navigateToAuditLog(testDagId);

    const initialEvents = await eventsPage.getEventTypes(true);

    await eventsPage.clickColumnToSort("Event");

    const sortedEvents = await eventsPage.getEventTypes(true);

    const expectedSorted = [...initialEvents].sort();

    expect(sortedEvents).toEqual(expectedSorted);
  });
});
