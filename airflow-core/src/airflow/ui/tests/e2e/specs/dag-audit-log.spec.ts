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

  test.setTimeout(60_000);

  test.beforeAll(async ({ browser }) => {
    test.setTimeout(7 * 60 * 1000);
    const context = await browser.newContext({ storageState: AUTH_FILE });
    const page = await context.newPage();
    const setupDagsPage = new DagsPage(page);
    const setupEventsPage = new EventsPage(page);

    for (let i = 0; i < 10; i++) {
      await setupDagsPage.triggerDag(testDagId);
    }

    await setupEventsPage.navigateToAuditLog(testDagId);
    await context.close();
  });

  test.beforeEach(({ page }) => {
    eventsPage = new EventsPage(page);
  });

  test("should navigate to audit log tab and display table", async () => {
    await eventsPage.navigateToAuditLog(testDagId);

    const rowCount = await eventsPage.getEventLogCount();

    await expect(eventsPage.eventsTable).toBeVisible();

    const isTableVisible = await eventsPage.isAuditLogTableVisible();

    expect(isTableVisible).toBe(true);
    expect(rowCount).toBeGreaterThanOrEqual(0);
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

    if (rows.length > 0) {
      const [firstRow] = rows;

      if (firstRow) {
        const cells = firstRow.locator("td");
        const cellCount = await cells.count();

        expect(cellCount).toBeGreaterThan(0);

        const allTextContents = await cells.allTextContents();
        const hasContent = allTextContents.some((text) => text.trim().length > 0);

        expect(hasContent).toBe(true);
      }
    }
  });

  test("should paginate through audit log entries", async () => {
    await eventsPage.navigateToAuditLog(testDagId);

    const hasNext = await eventsPage.hasNextPage();

    if (hasNext) {
      const initialEvents = await eventsPage.getEventTypes();

      await eventsPage.clickNextPage();

      const nextPageEvents = await eventsPage.getEventTypes();

      expect(nextPageEvents).not.toEqual(initialEvents);

      await eventsPage.clickPrevPage();

      const backToFirstEvents = await eventsPage.getEventTypes();

      expect(backToFirstEvents).toEqual(initialEvents);
    }
  });

  test("should sort audit log entries when clicking column header", async () => {
    await eventsPage.navigateToAuditLog(testDagId);

    const initialEvents = await eventsPage.getEventTypes();

    await eventsPage.clickColumnToSort("Event");

    const sortedEvents = await eventsPage.getEventTypes();

    const orderChanged = JSON.stringify(initialEvents) !== JSON.stringify(sortedEvents);

    expect(orderChanged).toBe(true);
  });
});
