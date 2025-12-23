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
import { BackfillPage } from "tests/e2e/pages/BackfillPage";

/**
 * Backfill E2E Tests
 */

const getPastDate = (daysAgo: number): string => {
  const date = new Date();

  date.setDate(date.getDate() - daysAgo);

  return date.toISOString().slice(0, 16);
};

test.describe.serial("Backfill Tabs", () => {
  let backfillPage: BackfillPage;

  const allRunsDagId = "example_nested_branch_dag";
  const missingRunsDagId = "example_branch_labels";
  const missingAndErroredRunsDagId = "example_skip_dag";

  test.beforeEach(({ page }) => {
    backfillPage = new BackfillPage(page);
  });

  test("verify date range selection (start date, end date)", async () => {
    // Set start date after end date to trigger validation error
    const fromDate = getPastDate(1);
    const toDate = getPastDate(7);

    await backfillPage.navigateToDagDetail(allRunsDagId);
    await backfillPage.openBackfillDialog();

    await backfillPage.backfillFromDateInput.fill(fromDate);
    await backfillPage.backfillToDateInput.fill(toDate);

    await expect(backfillPage.backfillDateError).toBeVisible();
  });

  test("should create backfill with 'All Runs' behavior", async () => {
    const fromDate = getPastDate(2);
    const toDate = getPastDate(1);
    const expectedFromDate = fromDate.replace("T", " ");
    const expectedToDate = toDate.replace("T", " ");

    await backfillPage.createBackfill(allRunsDagId, { fromDate, reprocessBehavior: "All Runs", toDate });
    await backfillPage.verifyBackfillCreated({
      dagName: allRunsDagId,
      expectedFromDate,
      expectedToDate,
      reprocessBehavior: "All Runs",
    });
  });

  test("should create backfill with 'Missing Runs' behavior", async () => {
    const fromDate = getPastDate(40);
    const toDate = getPastDate(30);
    const expectedFromDate = fromDate.replace("T", " ");
    const expectedToDate = toDate.replace("T", " ");

    await backfillPage.createBackfill(missingRunsDagId, {
      fromDate,
      reprocessBehavior: "Missing Runs",
      toDate,
    });
    await backfillPage.verifyBackfillCreated({
      dagName: missingRunsDagId,
      expectedFromDate,
      expectedToDate,
      reprocessBehavior: "Missing Runs",
    });
  });

  test("should create backfill with 'Missing and Errored Runs' behavior", async () => {
    const fromDate = getPastDate(55);
    const toDate = getPastDate(50);
    const expectedFromDate = fromDate.replace("T", " ");
    const expectedToDate = toDate.replace("T", " ");

    await backfillPage.createBackfill(missingAndErroredRunsDagId, {
      fromDate,
      reprocessBehavior: "Missing and Errored Runs",
      toDate,
    });
    await backfillPage.verifyBackfillCreated({
      dagName: missingAndErroredRunsDagId,
      expectedFromDate,
      expectedToDate,
      reprocessBehavior: "Missing and Errored Runs",
    });
  });
});
