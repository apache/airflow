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
import { DagRunsTabPage } from "tests/e2e/pages/DagRunsTabPage";

test.describe("DAG Runs Tab", () => {
  test.setTimeout(60_000);

  let dagRunsTabPage: DagRunsTabPage;
  const testDagId = testConfig.testDag.id;

  test.beforeAll(async ({ browser }) => {
    test.setTimeout(3 * 60 * 1000);
    const context = await browser.newContext({ storageState: AUTH_FILE });
    const page = await context.newPage();
    const setupPage = new DagRunsTabPage(page);

    await setupPage.navigateToDag(testDagId);
    const runId1 = await setupPage.triggerDagRun();

    if (runId1 !== undefined) {
      await setupPage.navigateToRunDetails(testDagId, runId1);
      await setupPage.markRunAs("success");
    }

    await setupPage.navigateToDag(testDagId);
    const runId2 = await setupPage.triggerDagRun();

    if (runId2 !== undefined) {
      await setupPage.navigateToRunDetails(testDagId, runId2);
      await setupPage.markRunAs("failed");
    }

    await context.close();
  });

  test.beforeEach(({ page }) => {
    dagRunsTabPage = new DagRunsTabPage(page);
  });

  test("navigate to DAG detail page and click Runs tab", async () => {
    await dagRunsTabPage.navigateToDag(testDagId);
    await dagRunsTabPage.clickRunsTab();

    await expect(dagRunsTabPage.page).toHaveURL(/\/dags\/.*\/runs/);
  });

  test("verify run details display correctly", async () => {
    await dagRunsTabPage.navigateToDag(testDagId);
    await dagRunsTabPage.clickRunsTab();
    await dagRunsTabPage.verifyRunDetailsDisplay();
  });

  test("verify runs exist in table", async () => {
    await dagRunsTabPage.navigateToDag(testDagId);
    await dagRunsTabPage.clickRunsTab();
    await dagRunsTabPage.verifyRunsExist();
  });

  test("click on a run and verify run details page", async () => {
    await dagRunsTabPage.navigateToDag(testDagId);
    await dagRunsTabPage.clickRunsTab();
    await dagRunsTabPage.clickRunAndVerifyDetails();
  });

  test("filter runs by success state", async () => {
    await dagRunsTabPage.navigateToDag(testDagId);
    await dagRunsTabPage.clickRunsTab();
    await dagRunsTabPage.filterByState("success");
    await dagRunsTabPage.verifyFilteredByState("success");
  });

  test("filter runs by failed state", async () => {
    await dagRunsTabPage.navigateToDag(testDagId);
    await dagRunsTabPage.clickRunsTab();
    await dagRunsTabPage.filterByState("failed");
    await dagRunsTabPage.verifyFilteredByState("failed");
  });

  test("search for dag run by run ID pattern", async () => {
    await dagRunsTabPage.navigateToDag(testDagId);
    await dagRunsTabPage.clickRunsTab();
    await dagRunsTabPage.searchByRunIdPattern("manual");
    await dagRunsTabPage.verifySearchResults("manual");
  });

  test("paginate through runs", async () => {
    await dagRunsTabPage.clickRunsTabWithPageSize(testDagId, 2);

    const initialRowCount = await dagRunsTabPage.getRowCount();

    expect(initialRowCount).toBe(2);

    await dagRunsTabPage.clickNextPage();

    const nextPageRowCount = await dagRunsTabPage.getRowCount();

    expect(nextPageRowCount).toBeGreaterThan(0);

    await dagRunsTabPage.clickPrevPage();

    const backRowCount = await dagRunsTabPage.getRowCount();

    expect(backRowCount).toBe(initialRowCount);
  });
});
