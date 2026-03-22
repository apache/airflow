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
import { test, expect } from "@playwright/test";
import { testConfig, AUTH_FILE } from "playwright.config";
import { DagsPage } from "tests/e2e/pages/DagsPage";
import {
  apiCreateDagRun,
  apiSetDagRunState,
  uniqueRunId,
  waitForDagReady,
} from "tests/e2e/utils/test-helpers";

test.describe("Dag Tasks Tab", () => {
  const testDagId = testConfig.testDag.id;

  // API-based setup replaces UI-based triggerDag/verifyDagRunStatus to avoid
  // dialog race conditions and 7-minute timeout polling via page reloads.
  test.beforeAll(async ({ browser }) => {
    test.setTimeout(120_000);

    const context = await browser.newContext({ storageState: AUTH_FILE });
    const page = await context.newPage();

    await waitForDagReady(page, testDagId);

    const runId = uniqueRunId("tasks_run");

    await apiCreateDagRun(page, testDagId, {
      dag_run_id: runId,
      logical_date: new Date().toISOString(),
    });
    await apiSetDagRunState(page, { dagId: testDagId, runId, state: "success" });

    await context.close();
  });
  test("verify tasks tab displays task list", async ({ page }) => {
    const dagPage = new DagsPage(page);

    await dagPage.navigateToDagTasks(testDagId);

    await expect(page).toHaveURL(/\/tasks$/);
    await expect(dagPage.taskRows.first()).toBeVisible();

    const firstRow = dagPage.taskRows.first();

    await expect(firstRow.getByRole("link").first()).toBeVisible();
    await expect(firstRow).toContainText("BashOperator");
    await expect(firstRow).toContainText("all_success");
  });

  test("verify search tasks by name", async ({ page }) => {
    const dagPage = new DagsPage(page);

    await dagPage.navigateToDagTasks(testDagId);

    const firstTaskLink = dagPage.taskRows.first().getByRole("link").first();
    const taskName = await firstTaskLink.textContent();

    if (taskName === null) {
      throw new Error("Task name not found");
    }

    await dagPage.searchBox.fill(taskName);

    await expect.poll(() => dagPage.taskRows.count(), { timeout: 20_000 }).toBe(1);
    await expect(dagPage.taskRows).toContainText(taskName);
  });

  test("verify filter tasks by operator dropdown", async ({ page }) => {
    const dagPage = new DagsPage(page);

    await dagPage.navigateToDagTasks(testDagId);

    let operators: Array<string> = [];

    await expect
      .poll(
        async () => {
          operators = await dagPage.getFilterOptions(dagPage.operatorFilter);

          return operators.length;
        },
        { timeout: 10_000 },
      )
      .toBeGreaterThan(0);

    for (const operator of operators) {
      await dagPage.filterByOperator(operator);

      // Use allTextContents() instead of nth(i).textContent() to avoid
      // stale DOM references when React re-renders the table mid-iteration.
      await expect
        .poll(
          async () => {
            const texts = await dagPage.taskRows.allTextContents();

            if (texts.length === 0) return false;

            return texts.every((text) => text.includes(operator));
          },
          { timeout: 20_000 },
        )
        .toBeTruthy();

      await dagPage.navigateToDagTasks(testDagId);
    }
  });

  test("verify filter tasks by trigger rule dropdown", async ({ page }) => {
    const dagPage = new DagsPage(page);

    await dagPage.navigateToDagTasks(testDagId);

    let rules: Array<string> = [];

    await expect
      .poll(
        async () => {
          rules = await dagPage.getFilterOptions(dagPage.triggerRuleFilter);

          return rules.length;
        },
        { timeout: 10_000 },
      )
      .toBeGreaterThan(0);

    for (const rule of rules) {
      await dagPage.filterByTriggerRule(rule);

      await expect
        .poll(
          async () => {
            const texts = await dagPage.taskRows.allTextContents();

            if (texts.length === 0) return false;

            return texts.every((text) => text.includes(rule));
          },
          { timeout: 20_000 },
        )
        .toBeTruthy();

      await dagPage.navigateToDagTasks(testDagId);
    }
  });

  test("verify filter by retries", async ({ page }) => {
    const dagPage = new DagsPage(page);

    await dagPage.navigateToDagTasks(testDagId);

    const retriesOptions = await dagPage.getFilterOptions(dagPage.retriesFilter);

    if (retriesOptions.length === 0) {
      return;
    }

    for (const retries of retriesOptions) {
      await dagPage.filterByRetries(retries);
      await expect(dagPage.taskRows.first()).toBeVisible();
      await dagPage.navigateToDagTasks(testDagId);
    }
  });
  test("verify click task to show details", async ({ page }) => {
    const dagPage = new DagsPage(page);

    await dagPage.navigateToDagTasks(testDagId);

    const firstCard = dagPage.taskRows.first();
    const taskLink = firstCard.getByRole("link").first();

    await taskLink.click();
    await expect(page).toHaveURL(new RegExp(`/dags/${testDagId}/tasks/.*`));
  });
});
