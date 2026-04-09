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
import { test } from "@playwright/test";
import { AUTH_FILE, testConfig } from "playwright.config";
import { DagsPage } from "tests/e2e/pages/DagsPage";
import { GridPage } from "tests/e2e/pages/GridPage";

test.describe("DAG Grid View", () => {
  test.setTimeout(60_000);

  let gridPage: GridPage;
  const testDagId = testConfig.testDag.id;

  test.beforeAll(async ({ browser }) => {
    test.setTimeout(3 * 60 * 1000);
    const context = await browser.newContext({ storageState: AUTH_FILE });
    const page = await context.newPage();
    const setupDagsPage = new DagsPage(page);

    const dagRunId = await setupDagsPage.triggerDag(testDagId);

    if (dagRunId !== null) {
      await setupDagsPage.verifyDagRunStatus(testDagId, dagRunId);
    }

    await context.close();
  });

  test.beforeEach(({ page }) => {
    gridPage = new GridPage(page);
  });

  test("navigate to DAG detail page and display grid view", async () => {
    await gridPage.navigateToDag(testDagId);
    await gridPage.switchToGridView();
    await gridPage.verifyGridViewIsActive();
  });

  test("render grid with task instances", async () => {
    await gridPage.navigateToDag(testDagId);
    await gridPage.switchToGridView();
    await gridPage.verifyGridHasTaskInstances();
  });

  test("display task states with color coding", async () => {
    await gridPage.navigateToDag(testDagId);
    await gridPage.switchToGridView();
    await gridPage.verifyTaskStatesAreColorCoded();
  });

  test("show task details when clicking a grid cell", async () => {
    await gridPage.navigateToDag(testDagId);
    await gridPage.switchToGridView();
    await gridPage.clickGridCellAndVerifyDetails();
  });

  test("show tooltip on grid cell hover", async () => {
    await gridPage.navigateToDag(testDagId);
    await gridPage.switchToGridView();
    await gridPage.verifyTaskTooltipOnHover();
  });
});
