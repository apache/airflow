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
import { XComsPage } from "tests/e2e/pages/XComsPage";
import {
  apiCreateDagRun,
  uniqueRunId,
  waitForDagReady,
  waitForDagRunStatus,
  waitForTableLoad,
} from "tests/e2e/utils/test-helpers";

test.describe("XComs Page", () => {
  test.setTimeout(60_000);

  let xcomsPage: XComsPage;
  const testDagId = testConfig.xcomDag.id;
  const testXComKey = "return_value";
  const triggerCount = 2;

  // API-based setup replaces UI-based triggerDag/verifyDagRunStatus to avoid
  // 7-minute timeout polling via page reloads.
  test.beforeAll(async ({ browser }) => {
    test.setTimeout(3 * 60 * 1000);
    const context = await browser.newContext({ storageState: AUTH_FILE });
    const page = await context.newPage();

    await waitForDagReady(page, testDagId);

    // Unpause the DAG so the scheduler can execute it and produce XCom values
    const baseUrl = process.env.AIRFLOW_UI_BASE_URL ?? "http://localhost:28080";

    await page.request.patch(`${baseUrl}/api/v2/dags/${testDagId}`, {
      data: { is_paused: false },
    });

    // Trigger runs and wait for completion via API
    for (let i = 0; i < triggerCount; i++) {
      const runId = uniqueRunId(`xcom_run_${i}`);

      await apiCreateDagRun(page, testDagId, {
        dag_run_id: runId,
        logical_date: new Date(Date.now() + i * 60_000).toISOString(),
      });
      await waitForDagRunStatus(page, {
        dagId: testDagId,
        expectedState: "success",
        runId,
        timeout: 120_000,
      });
    }

    // Verify XComs table has data using Playwright locator instead of document.querySelector
    const setupXComsPage = new XComsPage(page);

    await setupXComsPage.navigate();
    await waitForTableLoad(page, { timeout: 30_000 });

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
