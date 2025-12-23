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
import { LoginPage } from "tests/e2e/pages/LoginPage";

/**
 * DAG Audit Log E2E Tests
 */

test.describe("DAG Audit Log", () => {
  let loginPage: LoginPage;
  let eventsPage: EventsPage;

  const testCredentials = testConfig.credentials;
  const testDagId = testConfig.testDag.id;

  test.beforeEach(({ page }) => {
    loginPage = new LoginPage(page);
    eventsPage = new EventsPage(page);
  });

  test("should navigate to audit log tab and display table", async () => {
    await loginPage.navigateAndLogin(testCredentials.username, testCredentials.password);

    await loginPage.expectLoginSuccess();

    await eventsPage.navigateToAuditLog(testDagId);

    await expect(eventsPage.eventsTable).toBeVisible();

    const isTableVisible = await eventsPage.isAuditLogTableVisible();

    expect(isTableVisible).toBe(true);

    const rowCount = await eventsPage.getEventLogCount();

    expect(rowCount).toBeGreaterThanOrEqual(0);
  });

  test("should display all expected columns in audit log table", async () => {
    await loginPage.navigateAndLogin(testCredentials.username, testCredentials.password);

    await loginPage.expectLoginSuccess();

    await eventsPage.navigateToAuditLog(testDagId);

    // Verify core columns are visible
    await expect(eventsPage.whenColumn).toBeVisible();
    await expect(eventsPage.eventColumn).toBeVisible();
    await expect(eventsPage.ownerColumn).toBeVisible();
    await expect(eventsPage.extraColumn).toBeVisible();

    // In DAG context, DAG ID column should NOT be visible
    const dagIdColumn = eventsPage.eventsTable.locator('th:has-text("DAG ID")');

    await expect(dagIdColumn).not.toBeVisible();
  });

  test("should display audit log entries with valid data", async () => {
    await loginPage.navigateAndLogin(testCredentials.username, testCredentials.password);

    await loginPage.expectLoginSuccess();

    await eventsPage.navigateToAuditLog(testDagId);

    const rows = await eventsPage.getEventLogRows();

    // If there are rows, verify they contain valid data
    if (rows.length > 0) {
      const [firstRow] = rows;

      if (firstRow) {
        const cells = firstRow.locator("td");
        const cellCount = await cells.count();

        expect(cellCount).toBeGreaterThan(0);

        // Verify the first row has at least some data
        const allTextContents = await cells.allTextContents();
        const hasContent = allTextContents.some((text) => text.trim().length > 0);

        expect(hasContent).toBe(true);
      }
    }
  });

  test("should paginate through audit log entries", async () => {
    await loginPage.navigateAndLogin(testCredentials.username, testCredentials.password);

    await loginPage.expectLoginSuccess();

    await eventsPage.navigateToAuditLog(testDagId);

    const rowCount = await eventsPage.getEventLogCount();

    // Only test pagination if there are rows
    if (rowCount === 0) {
      test.skip();
    }

    const hasNext = await eventsPage.hasNextPage();

    // Only test pagination if next page is available
    if (hasNext) {
      const initialEvents = await eventsPage.getEventTypes();

      await eventsPage.clickNextPage();

      const nextPageEvents = await eventsPage.getEventTypes();

      // Verify that the events changed after clicking next
      expect(nextPageEvents).not.toEqual(initialEvents);

      await eventsPage.clickPrevPage();

      const backToFirstEvents = await eventsPage.getEventTypes();

      // Verify we're back to the original page
      expect(backToFirstEvents).toEqual(initialEvents);
    }
  });

  test("should sort audit log entries when clicking column header", async () => {
    await loginPage.navigateAndLogin(testCredentials.username, testCredentials.password);

    await loginPage.expectLoginSuccess();

    await eventsPage.navigateToAuditLog(testDagId);

    const rowCount = await eventsPage.getEventLogCount();

    // Only test sorting if there are multiple rows
    if (rowCount < 2) {
      test.skip();
    }

    // Get initial event types
    const initialEvents = await eventsPage.getEventTypes();

    // Click the Event column to sort
    await eventsPage.clickColumnToSort("Event");

    // Get events after sorting
    const sortedEvents = await eventsPage.getEventTypes();

    // Verify that the order changed
    const orderChanged = JSON.stringify(initialEvents) !== JSON.stringify(sortedEvents);

    expect(orderChanged).toBe(true);
  });
});
