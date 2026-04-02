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
import { AUTH_FILE, testConfig } from "playwright.config";
import { DagsPage } from "tests/e2e/pages/DagsPage";
import { XComsPage } from "tests/e2e/pages/XComsPage";

test.describe("XComs Page", () => {
  test.setTimeout(60_000);

  let xcomsPage: XComsPage;
  const testDagId = testConfig.xcomDag.id;
  const testXComKey = "return_value";
  const triggerCount = 2;

  test.beforeAll(async ({ browser }) => {
    test.setTimeout(3 * 60 * 1000);
    const context = await browser.newContext({ storageState: AUTH_FILE });
    const page = await context.newPage();
    const setupDagsPage = new DagsPage(page);
    const setupXComsPage = new XComsPage(page);

    for (let i = 0; i < triggerCount; i++) {
      const dagRunId = await setupDagsPage.triggerDag(testDagId);

      await setupDagsPage.verifyDagRunStatus(testDagId, dagRunId);
    }

    await setupXComsPage.navigate();
    await page.waitForFunction(
      (minCount) => {
        const table = document.querySelector('[data-testid="table-list"]');

        if (!table) {
          return false;
        }
        const rows = table.querySelectorAll("tbody tr");

        return rows.length >= minCount;
      },
      triggerCount,
      { timeout: 120_000 },
    );

    await context.close();
  });

  test.beforeEach(({ page }) => {
    xcomsPage = new XComsPage(page);
  });

  test("verify XComs table renders", async () => {
    await xcomsPage.navigate();
    await expect(xcomsPage.xcomsTable).toBeVisible();
  });

  test("verify XComs table displays data", async () => {
    await xcomsPage.navigate();
    await xcomsPage.verifyXComsExist();
  });

  test("verify XCom details display correctly", async () => {
    await xcomsPage.navigate();
    await xcomsPage.verifyXComDetailsDisplay();
  });

  test("verify XCom values can be viewed", async () => {
    await xcomsPage.navigate();
    await xcomsPage.verifyXComValuesDisplayed();
  });

  test("verify expand/collapse functionality", async () => {
    await xcomsPage.verifyExpandCollapse();
  });

  test("verify filtering by key pattern", async () => {
    await xcomsPage.verifyKeyPatternFiltering(testXComKey);
  });

  test("verify filtering by DAG display name", async () => {
    await xcomsPage.verifyDagDisplayNameFiltering(testDagId);
  });
});
