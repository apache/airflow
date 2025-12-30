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
import { DagsPage } from "tests/e2e/pages/DagsPage";

const DAG_ID = "example_bash_operator";

test.describe("Dag Tasks Tab", () => {
  test("should navigate to tasks tab and show list", async ({ page }) => {
    const dagPage = new DagsPage(page);

    await dagPage.navigateToDagTasks(DAG_ID);

    await expect(dagPage.tasksTab).toHaveAttribute("class", "active");
    await expect(dagPage.taskCards.first()).toBeVisible();
  });

  test("should search tasks by name", async ({ page }) => {
    const dagPage = new DagsPage(page);

    await dagPage.navigateToDagTasks(DAG_ID);

    await dagPage.searchBox.fill("runme_0");

    // Wait for filter to apply
    await expect(dagPage.taskCards).toHaveCount(1);
    await expect(dagPage.taskCards).toContainText("runme_0");
  });

  test("should filter tasks by operator", async ({ page }) => {
    const dagPage = new DagsPage(page);

    await dagPage.navigateToDagTasks(DAG_ID);

    await dagPage.filterByOperator("BashOperator");

    await expect(dagPage.taskCards.first()).toBeVisible();
    await expect(dagPage.taskCards.first()).toContainText("BashOperator");
  });

  test("should filter tasks by trigger rule", async ({ page }) => {
    const dagPage = new DagsPage(page);

    await dagPage.navigateToDagTasks(DAG_ID);

    await dagPage.filterByTriggerRule("all_success");

    await expect(dagPage.taskCards.first()).toBeVisible();
    await expect(dagPage.taskCards.first()).toContainText("all_success");
  });

  test("should filter tasks by retries", async ({ page }) => {
    const dagPage = new DagsPage(page);

    await dagPage.navigateToDagTasks(DAG_ID);

    await dagPage.retriesFilter.click();
    const option = page.locator('div[role="option"]').first();

    if (await option.isVisible()) {
      await option.click();
      await page.keyboard.press("Escape");
      await expect(dagPage.taskCards.first()).toBeVisible();
    } else {
      await dagPage.retriesFilter.click();
    }
  });

  test("should filter tasks by mapped status", async ({ page }) => {
    const dagPage = new DagsPage(page);

    await dagPage.navigateToDagTasks(DAG_ID);

    await dagPage.filterByMappedStatus("Not mapped");

    await expect(dagPage.taskCards.first()).toBeVisible();
  });

  test("should click task to show details", async ({ page }) => {
    const dagPage = new DagsPage(page);

    await dagPage.navigateToDagTasks(DAG_ID);

    const firstCard = dagPage.taskCards.first();
    const taskLink = firstCard.locator("a").first();

    await taskLink.click();
    await expect(page).toHaveURL(new RegExp(`/dags/${DAG_ID}/tasks/.*`));
  });
});
