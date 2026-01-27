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

  test("verify audit log table displays", async () => {
    await eventsPage.navigateToAuditLog(testDagId);

    await expect(eventsPage.eventsTable).toBeVisible();

    const rowCount = await eventsPage.tableRows.count();

    expect(rowCount).toBeGreaterThan(0);
  });

  test("verify expected columns are visible", async () => {
    await eventsPage.navigateToAuditLog(testDagId);

    await expect(eventsPage.whenColumn).toBeVisible();
    await expect(eventsPage.eventColumn).toBeVisible();
    await expect(eventsPage.ownerColumn).toBeVisible();
    await expect(eventsPage.extraColumn).toBeVisible();

    const dagIdColumn = eventsPage.eventsTable.locator('th:has-text("DAG ID")');

    await expect(dagIdColumn).not.toBeVisible();
  });

  test("verify audit log entries display valid data", async () => {
    await eventsPage.navigateToAuditLog(testDagId);

    const rows = await eventsPage.getEventLogRows();

    expect(rows.length).toBeGreaterThan(0);

    const [firstRow] = rows;

    if (!firstRow) {
      throw new Error("No rows found");
    }

    const whenCell = await eventsPage.getCellByColumnName(firstRow, "When");
    const eventCell = await eventsPage.getCellByColumnName(firstRow, "Event");
    const userCell = await eventsPage.getCellByColumnName(firstRow, "User");

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

  test("verify pagination through audit log entries", async () => {
    await eventsPage.navigateToAuditLog(testDagId, 3);

    const hasNext = await eventsPage.hasNextPage();

    expect(hasNext).toBe(true);

    const urlPage1 = eventsPage.page.url();

    expect(urlPage1).toContain("offset=0");
    expect(urlPage1).toContain("limit=3");

    await eventsPage.clickNextPage();

    const urlPage2 = eventsPage.page.url();

    expect(urlPage2).toContain("limit=3");
    expect(urlPage2).not.toContain("offset=0");

    await eventsPage.clickPrevPage();

    const urlBackToPage1 = eventsPage.page.url();

    expect(urlBackToPage1).toContain("offset=0");
    expect(urlBackToPage1).toContain("limit=3");
  });

  test("verify sorting when clicking column header", async () => {
    await eventsPage.navigateToAuditLog(testDagId);

    await eventsPage.clickColumnToSort("Event");

    const sortedEvents = await eventsPage.getEventTypes(true);

    expect(sortedEvents.length).toBeGreaterThan(0);
    expect(sortedEvents).toEqual([...sortedEvents].sort());
  });
});
