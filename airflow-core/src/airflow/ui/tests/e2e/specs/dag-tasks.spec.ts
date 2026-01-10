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

  console.log("Using test DAG ID:", testDagId);
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
    await expect(dagPage.taskCards.first()).toBeVisible();

    const firstCard = dagPage.taskCards.first();

    await expect(firstCard.locator("a").first()).toBeVisible();
    await expect(firstCard).toContainText("Operator");
    await expect(firstCard).toContainText("Trigger Rule");
    await expect(firstCard).toContainText("Last Instance");
  });

  test("verify search tasks by name", async ({ page }) => {
    const dagPage = new DagsPage(page);

    await dagPage.navigateToDagTasks(testDagId);

    await dagPage.searchBox.fill("runme_0");

    // Wait for filter to apply
    await expect(dagPage.taskCards).toHaveCount(1);
    await expect(dagPage.taskCards).toContainText("runme_0");
  });

  test("verify filter tasks by operator dropdown", async ({ page }) => {
    const dagPage = new DagsPage(page);

    await dagPage.navigateToDagTasks(testDagId);

    await dagPage.filterByOperator("BashOperator");

    await expect(dagPage.taskCards.first()).toBeVisible();
    await expect(dagPage.taskCards.first()).toContainText("BashOperator");
  });

  test("verify filter tasks by trigger rule dropdown", async ({ page }) => {
    const dagPage = new DagsPage(page);

    await dagPage.navigateToDagTasks(testDagId);

    await dagPage.filterByTriggerRule("all_success");

    await expect(dagPage.taskCards.first()).toBeVisible();
  });

  test("verify filter by retries", async ({ page }) => {
    const dagPage = new DagsPage(page);

    await dagPage.navigateToDagTasks(testDagId);

    await dagPage.retriesFilter.click();
    const option = page.locator('div[role="option"]').first();

    // Skip if no retry options available for this DAG
    const hasOptions = await option.isVisible();

    if (!hasOptions) {
      test.skip(true, "No retry options available for test DAG");

      return;
    }

    await option.click();
    await expect(dagPage.taskCards.first()).toBeVisible();
  });
  test("verify click task to show details", async ({ page }) => {
    const dagPage = new DagsPage(page);

    await dagPage.navigateToDagTasks(testDagId);

    const firstCard = dagPage.taskCards.first();
    const taskLink = firstCard.locator("a").first();

    await taskLink.click();
    await expect(page).toHaveURL(new RegExp(`/dags/${testDagId}/tasks/.*`));
  });
});
