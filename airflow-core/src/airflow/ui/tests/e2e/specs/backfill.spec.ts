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
import { testConfig } from "playwright.config";
import { expect, test } from "tests/e2e/fixtures";
import { REPROCESS_API_TO_UI } from "tests/e2e/pages/BackfillPage";
import type { ReprocessBehaviorApi } from "tests/e2e/pages/BackfillPage";
import {
  apiCancelAllActiveBackfills,
  apiCreateBackfill,
  apiWaitForBackfillComplete,
  waitForDagReady,
} from "tests/e2e/utils/test-helpers";

// Fixed past dates avoid non-determinism from relative date calculations.
// Controls tests use wide, non-overlapping ranges so the scheduler cannot
// complete the backfill before the test interacts with it.
const FIXED_DATES = {
  controls: {
    cancel: { from: "2014-01-01T00:00:00Z", to: "2015-01-01T00:00:00Z" },
    cancelledNoResume: { from: "2016-01-01T00:00:00Z", to: "2017-01-01T00:00:00Z" },
    resumePause: { from: "2012-01-01T00:00:00Z", to: "2013-01-01T00:00:00Z" },
  },
  set1: { from: "2020-01-01T00:00:00Z", to: "2020-01-02T00:00:00Z" },
  set2: { from: "2020-02-01T00:00:00Z", to: "2020-02-03T00:00:00Z" },
  set3: { from: "2020-03-01T00:00:00Z", to: "2020-03-04T00:00:00Z" },
};

// All blocks share the same Dag, so they must run serially to avoid cross-block interference.
test.describe("Backfill", () => {
  test.describe.configure({ mode: "serial" });

  test.describe("Backfill creation and validation", () => {
    test.setTimeout(120_000);

    const testDagId = testConfig.testDag.id;

    const backfillConfigs: Array<{
      behavior: ReprocessBehaviorApi;
      dates: { from: string; to: string };
    }> = [
      { behavior: "completed", dates: FIXED_DATES.set1 },
      { behavior: "none", dates: FIXED_DATES.set2 },
      { behavior: "failed", dates: FIXED_DATES.set3 },
    ];

    test.beforeAll(async ({ authenticatedRequest }) => {
      test.setTimeout(300_000);

      await waitForDagReady(authenticatedRequest, testDagId);
      await authenticatedRequest.patch(`/api/v2/dags/${testDagId}`, {
        data: { is_paused: false },
      });
      await apiCancelAllActiveBackfills(authenticatedRequest, testDagId);

      for (const config of backfillConfigs) {
        const backfillId = await apiCreateBackfill(authenticatedRequest, testDagId, {
          fromDate: config.dates.from,
          reprocessBehavior: config.behavior,
          toDate: config.dates.to,
        });

        await apiWaitForBackfillComplete(authenticatedRequest, backfillId, 240_000);
      }
    });

    test.afterAll(async ({ authenticatedRequest }) => {
      await apiCancelAllActiveBackfills(authenticatedRequest, testDagId).catch(() => undefined);
    });

    for (const config of backfillConfigs) {
      test(`verify backfill with '${REPROCESS_API_TO_UI[config.behavior]}' behavior`, async ({
        backfillPage,
      }) => {
        await backfillPage.navigateToBackfillsTab(testDagId);

        const details = await backfillPage.getBackfillDetailsByDateRange({
          fromDate: config.dates.from,
          toDate: config.dates.to,
        });

        expect(details.fromDate.slice(0, 10)).toEqual(config.dates.from.slice(0, 10));
        expect(details.toDate.slice(0, 10)).toEqual(config.dates.to.slice(0, 10));
        expect(details.createdAt).not.toEqual("");
        expect(details.completedAt).not.toEqual("");
        expect(details.reprocessBehavior).toEqual(REPROCESS_API_TO_UI[config.behavior]);
      });
    }

    test("Verify backfill table filters", async ({ backfillPage }) => {
      await backfillPage.navigateToBackfillsTab(testDagId);

      const tableHeaders = backfillPage.backfillsTable.locator("thead th");

      await expect(tableHeaders).toHaveCount(7); // Initial state should have 7 columns
      const initialColumnCount = await tableHeaders.count();

      await expect(backfillPage.getFilterButton()).toBeVisible();

      await backfillPage.openFilterMenu();

      const filterMenuItems = backfillPage.page.getByRole("menuitem");

      await expect(filterMenuItems).not.toHaveCount(0);

      const firstMenuItem = filterMenuItems.first();
      const columnToToggle = (await firstMenuItem.textContent())?.trim() ?? "";

      expect(columnToToggle).not.toBe("");

      await backfillPage.toggleColumn(columnToToggle);
      await backfillPage.page.keyboard.press("Escape");

      await expect(backfillPage.getColumnHeader(columnToToggle)).not.toBeVisible();

      await expect(tableHeaders).toHaveCount(initialColumnCount - 1);

      await backfillPage.openFilterMenu();
      await backfillPage.toggleColumn(columnToToggle);
      await backfillPage.page.keyboard.press("Escape");

      await expect(backfillPage.getColumnHeader(columnToToggle)).toBeVisible();

      await expect(tableHeaders).toHaveCount(initialColumnCount);
    });
  });

  test.describe("validate date range", () => {
    test.setTimeout(30_000);

    const testDagId = testConfig.testDag.id;

    test("verify date range selection (start date, end date)", async ({ backfillPage }) => {
      await backfillPage.navigateToDagDetail(testDagId);
      await backfillPage.openBackfillDialog();
      await backfillPage.backfillFromDateInput.fill("2025-01-10T00:00");
      await backfillPage.backfillToDateInput.fill("2025-01-01T00:00");
      await expect(backfillPage.backfillDateError).toBeVisible();
    });
  });

  test.describe("Backfill pause, resume, and cancel controls", () => {
    test.describe.configure({ mode: "serial" });
    test.setTimeout(120_000);

    const testDagId = testConfig.testDag.id;

    test.beforeEach(async ({ backfillPage }) => {
      await backfillPage.cancelAllActiveBackfillsViaApi(testDagId);
      await backfillPage.waitForNoActiveBackfillViaApi(testDagId, 30_000);
    });

    test.afterEach(async ({ backfillPage }) => {
      await backfillPage.cancelAllActiveBackfillsViaApi(testDagId);
    });

    test("verify pause and resume backfill", async ({ backfillPage }) => {
      const dates = FIXED_DATES.controls.resumePause;

      // Create + pause atomically to eliminate race with scheduler.
      await backfillPage.createPausedBackfillViaApi(testDagId, {
        fromDate: dates.from,
        reprocessBehavior: "completed",
        toDate: dates.to,
      });

      // Navigate to verify UI reflects the paused state, then test toggle cycle.
      await backfillPage.navigateToDagDetail(testDagId);
      await expect(backfillPage.unpauseButton).toBeVisible({ timeout: 15_000 });

      await backfillPage.togglePauseState();
      await expect(backfillPage.pauseButton).toBeVisible();

      await backfillPage.togglePauseState();
      await expect(backfillPage.unpauseButton).toBeVisible();
    });

    test("verify cancel backfill", async ({ backfillPage }) => {
      const dates = FIXED_DATES.controls.cancel;

      // Create + pause atomically to eliminate race with scheduler.
      await backfillPage.createPausedBackfillViaApi(testDagId, {
        fromDate: dates.from,
        reprocessBehavior: "completed",
        toDate: dates.to,
      });

      await backfillPage.navigateToDagDetail(testDagId);
      await expect(backfillPage.unpauseButton).toBeVisible({ timeout: 15_000 });

      await backfillPage.clickCancelButton();
      await expect(backfillPage.pauseOrUnpauseButton).not.toBeVisible();
      await expect(backfillPage.cancelButton).not.toBeVisible();
    });

    test("verify cancelled backfill cannot be resumed", async ({ backfillPage }) => {
      const dates = FIXED_DATES.controls.cancelledNoResume;

      // Setup via API: create and cancel directly (UI cancel is tested above).
      const backfillId = await backfillPage.createBackfillViaApi(testDagId, {
        fromDate: dates.from,
        maxActiveRuns: 1,
        reprocessBehavior: "completed",
        toDate: dates.to,
      });

      await backfillPage.cancelBackfillViaApi(backfillId);

      // Verify UI: no pause/resume controls visible after cancel.
      await backfillPage.navigateToDagDetail(testDagId);
      await expect(backfillPage.pauseOrUnpauseButton).not.toBeVisible();

      // Verify: completedAt is set in backfills table.
      await backfillPage.navigateToBackfillsTab(testDagId);

      const details = await backfillPage.getBackfillDetailsByDateRange({
        fromDate: dates.from,
        toDate: dates.to,
      });

      expect(details.completedAt).not.toBe("");
    });
  });
});
