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

test.describe("Dag Tasks Tab", () => {
  const testDagId = testConfig.testDag.id;

  test.beforeAll(async ({ browser }) => {
    test.setTimeout(7 * 60 * 1000);

    const context = await browser.newContext({ storageState: AUTH_FILE });
    const page = await context.newPage();
    const dagPage = new DagsPage(page);

    const dagRunId = await dagPage.triggerDag(testDagId);

    await dagPage.verifyDagRunStatus(testDagId, dagRunId);

    await context.close();
  });
  test("verify tasks tab displays task list", async ({ page }) => {
    const dagPage = new DagsPage(page);

    await dagPage.navigateToDagTasks(testDagId);

    await expect(page).toHaveURL(/\/tasks$/);
    await expect(dagPage.taskRows.first()).toBeVisible();

    const firstRow = dagPage.taskRows.first();

    await expect(firstRow.locator("a").first()).toBeVisible();
    await expect(firstRow).toContainText("BashOperator");
    await expect(firstRow).toContainText("all_success");
  });

  test("verify search tasks by name", async ({ page }) => {
    const dagPage = new DagsPage(page);

    await dagPage.navigateToDagTasks(testDagId);

    const firstTaskLink = dagPage.taskRows.first().locator("a").first();
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

    const operators = await dagPage.getFilterOptions(dagPage.operatorFilter);

    expect(operators.length).toBeGreaterThan(0);

    for (const operator of operators) {
      await dagPage.filterByOperator(operator);

      await expect
        .poll(
          async () => {
            const count = await dagPage.taskRows.count();

            if (count === 0) return false;
            for (let i = 0; i < count; i++) {
              const text = await dagPage.taskRows.nth(i).textContent();

              if (!text?.includes(operator)) return false;
            }

            return true;
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

    const rules = await dagPage.getFilterOptions(dagPage.triggerRuleFilter);

    expect(rules.length).toBeGreaterThan(0);

    for (const rule of rules) {
      await dagPage.filterByTriggerRule(rule);

      await expect
        .poll(
          async () => {
            const count = await dagPage.taskRows.count();

            if (count === 0) return false;
            for (let i = 0; i < count; i++) {
              const text = await dagPage.taskRows.nth(i).textContent();

              if (!text?.includes(rule)) return false;
            }

            return true;
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
    const taskLink = firstCard.locator("a").first();

    await taskLink.click();
    await expect(page).toHaveURL(new RegExp(`/dags/${testDagId}/tasks/.*`));
  });
});
