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
import { expect } from "tests/e2e/fixtures";
import { test } from "tests/e2e/fixtures/audit-log-data";

test.describe("DAG Audit Log", () => {
  test.setTimeout(60_000);

  test("verify audit log table displays", async ({ auditLogData, eventsPage }) => {
    await eventsPage.navigateToAuditLog(auditLogData.dagId);

    await expect(eventsPage.eventsTable).toBeVisible();
    await expect(eventsPage.tableRows).not.toHaveCount(0);
  });

  test("verify expected columns are visible", async ({ auditLogData, eventsPage }) => {
    await eventsPage.navigateToAuditLog(auditLogData.dagId);

    await expect(eventsPage.whenColumn).toBeVisible();
    await expect(eventsPage.eventColumn).toBeVisible();
    await expect(eventsPage.ownerColumn).toBeVisible();
    await expect(eventsPage.extraColumn).toBeVisible();

    const dagIdColumn = eventsPage.eventsTable.getByRole("columnheader").filter({ hasText: "DAG ID" });

    await expect(dagIdColumn).not.toBeVisible();
  });

  test("verify audit log entries display valid data", async ({ auditLogData, eventsPage }) => {
    await eventsPage.navigateToAuditLog(auditLogData.dagId);

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
