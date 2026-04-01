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

  test.setTimeout(120_000);

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
    await expect(async () => {
      const count = await setupEventsPage.tableRows.count();

      expect(count).toBeGreaterThanOrEqual(expectedEventCount);
    }).toPass({ timeout: 60_000 });

    await context.close();
  });

  test.beforeEach(({ page }) => {
    eventsPage = new EventsPage(page);
  });

  test("verify audit log table displays", async () => {
    await eventsPage.navigateToAuditLog(testDagId);

    await expect(eventsPage.eventsTable).toBeVisible();
    await expect(eventsPage.tableRows).not.toHaveCount(0);
  });

  test("verify expected columns are visible", async () => {
    await eventsPage.navigateToAuditLog(testDagId);

    await expect(eventsPage.whenColumn).toBeVisible();
    await expect(eventsPage.eventColumn).toBeVisible();
    await expect(eventsPage.ownerColumn).toBeVisible();
    await expect(eventsPage.extraColumn).toBeVisible();

    const dagIdColumn = eventsPage.eventsTable.getByRole("columnheader").filter({ hasText: "DAG ID" });

    await expect(dagIdColumn).not.toBeVisible();
  });

  test.fixme("verify audit log entries display valid data", async () => {
    await eventsPage.navigateToAuditLog(testDagId);

    await expect(eventsPage.tableRows).not.toHaveCount(0);

    const firstRow = eventsPage.tableRows.first();
    const whenCell = await eventsPage.getCellByColumnName(firstRow, "When");
    const eventCell = await eventsPage.getCellByColumnName(firstRow, "Event");
    const userCell = await eventsPage.getCellByColumnName(firstRow, "User");

    await expect(whenCell).toHaveText(/\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}/);
    await expect(eventCell).toHaveText(/[a-z][_a-z]*/);
    await expect(userCell).toHaveText(/\w+/);
  });
});
