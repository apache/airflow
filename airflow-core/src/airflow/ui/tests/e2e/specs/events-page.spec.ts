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
import { expect, test } from "tests/e2e/fixtures";

test.describe("Events Page", () => {
  test("verify events page displays correctly", async ({ eventsPage }) => {
    await eventsPage.navigate();

    await expect(eventsPage.eventsPageTitle).toBeVisible({ timeout: 10_000 });
    await expect(eventsPage.eventsTable).toBeVisible();
    await eventsPage.verifyTableColumns();
  });

  test("verify search input is visible", async ({ eventsPage }) => {
    await eventsPage.navigate();
    await eventsPage.waitForEventsTable();

    await expect(eventsPage.filterBar).toBeVisible({ timeout: 10_000 });
    await eventsPage.verifyFilterMenuHasOptions();
  });
});

test.describe("Events with Generated Data", () => {
  test.setTimeout(60_000);

  test("verify audit log entries display valid data", async ({ eventsPage, executedDagRun: _run }) => {
    await eventsPage.navigate();

    await expect(eventsPage.eventsTable).toBeVisible();

    const rowCount = await eventsPage.getTableRowCount();

    expect(rowCount).toBeGreaterThan(0);
    await eventsPage.verifyLogEntriesWithData();
  });

  test("verify search for specific event type and filtered results", async ({
    eventsPage,
    executedDagRun: _run,
  }) => {
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

  test("verify filter by Dag ID", async ({ eventsPage, executedDagRun }) => {
    await eventsPage.navigate();
    await eventsPage.addFilter("DAG ID");
    await eventsPage.setFilterValue("DAG ID", executedDagRun.dagId);
    await expect(eventsPage.eventsTable).toBeVisible();

    await expect(async () => {
      const rows = await eventsPage.getEventLogRows();

      expect(rows.length).toBeGreaterThan(0);

      await expect(eventsPage.eventsTable).toBeVisible();

      for (const row of rows) {
        const dagIdCell = await eventsPage.getCellByColumnName(row, "DAG ID");
        const dagIdText = await dagIdCell.textContent();

        expect(dagIdText?.toLowerCase()).toContain(executedDagRun.dagId.toLowerCase());
      }
    }).toPass({ timeout: 20_000 });
  });
});
