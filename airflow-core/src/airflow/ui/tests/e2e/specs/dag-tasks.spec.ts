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

test.describe("Dag Tasks Tab", () => {
  test("verify tasks tab displays task list", async ({ dagsPage, page, successDagRun }) => {
    await dagsPage.navigateToDagTasks(successDagRun.dagId);

    await expect(page).toHaveURL(/\/tasks$/);
    await expect(dagsPage.taskRows.first()).toBeVisible();

    const firstRow = dagsPage.taskRows.first();

    await expect(firstRow.getByRole("link").first()).toBeVisible();
    await expect(firstRow).toContainText("BashOperator");
    await expect(firstRow).toContainText("all_success");
  });

  test("verify search tasks by name", async ({ dagsPage, successDagRun }) => {
    await dagsPage.navigateToDagTasks(successDagRun.dagId);

    const firstTaskLink = dagsPage.taskRows.first().getByRole("link").first();
    const taskName = await firstTaskLink.textContent();

    if (taskName === null) {
      throw new Error("Task name not found");
    }

    await dagsPage.searchBox.fill(taskName);

    await expect.poll(() => dagsPage.taskRows.count(), { timeout: 20_000 }).toBe(1);
    await expect(dagsPage.taskRows).toContainText(taskName);
  });

  test("verify filter tasks by operator dropdown", async ({ dagsPage, successDagRun }) => {
    await dagsPage.navigateToDagTasks(successDagRun.dagId);

    let operators: Array<string> = [];

    await expect
      .poll(
        async () => {
          operators = await dagsPage.getFilterOptions(dagsPage.operatorFilter);

          return operators.length;
        },
        { timeout: 30_000 },
      )
      .toBeGreaterThan(0);

    for (const operator of operators) {
      await dagsPage.filterByOperator(operator);

      // Use allTextContents() instead of nth(i).textContent() to avoid
      // stale DOM references when React re-renders the table mid-iteration.
      await expect
        .poll(
          async () => {
            const texts = await dagsPage.taskRows.allTextContents();

            if (texts.length === 0) return false;

            return texts.every((text) => text.includes(operator));
          },
          { timeout: 20_000 },
        )
        .toBeTruthy();

      await dagsPage.navigateToDagTasks(successDagRun.dagId);
    }
  });

  test("verify filter tasks by trigger rule dropdown", async ({ dagsPage, successDagRun }) => {
    await dagsPage.navigateToDagTasks(successDagRun.dagId);

    let rules: Array<string> = [];

    await expect
      .poll(
        async () => {
          rules = await dagsPage.getFilterOptions(dagsPage.triggerRuleFilter);

          return rules.length;
        },
        { timeout: 30_000 },
      )
      .toBeGreaterThan(0);

    for (const rule of rules) {
      await dagsPage.filterByTriggerRule(rule);

      await expect
        .poll(
          async () => {
            const texts = await dagsPage.taskRows.allTextContents();

            if (texts.length === 0) return false;

            return texts.every((text) => text.includes(rule));
          },
          { timeout: 20_000 },
        )
        .toBeTruthy();

      await dagsPage.navigateToDagTasks(successDagRun.dagId);
    }
  });

  test("verify filter by retries", async ({ dagsPage, successDagRun }) => {
    await dagsPage.navigateToDagTasks(successDagRun.dagId);

    const retriesOptions = await dagsPage.getFilterOptions(dagsPage.retriesFilter);

    if (retriesOptions.length === 0) {
      return;
    }

    for (const retries of retriesOptions) {
      await dagsPage.filterByRetries(retries);
      await expect(dagsPage.taskRows.first()).toBeVisible();
      await dagsPage.navigateToDagTasks(successDagRun.dagId);
    }
  });

  test("verify click task to show details", async ({ dagsPage, page, successDagRun }) => {
    await dagsPage.navigateToDagTasks(successDagRun.dagId);

    const firstCard = dagsPage.taskRows.first();
    const taskLink = firstCard.getByRole("link").first();

    await expect(taskLink).toBeVisible({ timeout: 30_000 });

    await expect(async () => {
      await taskLink.click();
      await expect(page).toHaveURL(new RegExp(`/dags/${successDagRun.dagId}/tasks/.+`), { timeout: 5000 });
    }).toPass({ intervals: [1000, 2000], timeout: 30_000 });
  });
});
