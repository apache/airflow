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
