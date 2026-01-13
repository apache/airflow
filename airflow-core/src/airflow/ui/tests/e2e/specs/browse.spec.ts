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
import { AUTH_FILE } from "playwright.config";

import { BrowsePage } from "../pages/BrowsePage";

const hitlDagId = "example_hitl_operator";

test.describe("Verify Required Action page", () => {
  test.describe.configure({ mode: "serial" });

  test.beforeAll(async ({ browser }) => {
    test.setTimeout(120_000);

    const context = await browser.newContext({ storageState: AUTH_FILE });

    const page = await context.newPage();
    const browsePage = new BrowsePage(page);

    await browsePage.triggerAndMarkDagRun(hitlDagId, "success");
    await browsePage.triggerAndMarkDagRun(hitlDagId, "failed");
    await context.close();
  });

  test("Verify the actions list/table is displayed (or empty state if none)", async ({ page }) => {
    const browsePage = new BrowsePage(page);

    await browsePage.navigateToRequiredActionsPage();

    const isTableVisible = await browsePage.isTableDisplayed();
    const isEmptyStateVisible = await browsePage.isEmptyStateDisplayed();

    expect(isTableVisible || isEmptyStateVisible).toBe(true);

    if (isTableVisible) {
      await expect(browsePage.actionsTable).toBeVisible();

      const dagIdHeader = page.locator('th:has-text("Dag ID")');
      const taskIdHeader = page.locator('th:has-text("Task ID")');
      const dagRunIdHeader = page.locator('th:has-text("Dag Run ID")');
      const responseCreatedHeader = page.locator('th:has-text("Response created at")');
      const responseReceivedHeader = page.locator('th:has-text("Response received at")');

      await expect(dagIdHeader).toBeVisible();
      await expect(taskIdHeader).toBeVisible();
      await expect(dagRunIdHeader).toBeVisible();
      await expect(responseCreatedHeader).toBeVisible();
      await expect(responseReceivedHeader).toBeVisible();
    } else {
      await expect(browsePage.emptyStateMessage).toBeVisible();
    }
  });

  test("Verify Pagination", async ({ page }) => {
    const browsePage = new BrowsePage(page);

    await browsePage.navigateToRequiredActionsPage();

    const isTableVisible = await browsePage.isTableDisplayed();

    if (!isTableVisible) {
      return;
    }

    const isPaginationVisible = await browsePage.isPaginationVisible();

    if (!isPaginationVisible) {
      return;
    }

    await expect(browsePage.paginationNextButton).toBeVisible();
    await expect(browsePage.paginationPrevButton).toBeVisible();

    const initialSubjects = await browsePage.getActionSubjects();

    expect(initialSubjects.length).toBeGreaterThan(0);

    await browsePage.clickNextPage();

    const subjectsAfterNext = await browsePage.getActionSubjects();

    expect(subjectsAfterNext.length).toBeGreaterThan(0);
    expect(subjectsAfterNext).not.toEqual(initialSubjects);

    await browsePage.clickPrevPage();

    const subjectsAfterPrev = await browsePage.getActionSubjects();

    expect(subjectsAfterPrev).toEqual(initialSubjects);
  });
});
