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
import { LoginPage } from "tests/e2e/pages/LoginPage";

test.describe("Dag Run Tests", () => {
  const testDagId = testConfig.testDag.id;
  const testCredentials = testConfig.credentials;

  test.beforeAll(async ({ browser }) => {
    test.setTimeout(5 * 60 * 1000);

    const context = await browser.newContext({ storageState: AUTH_FILE });
    const page = await context.newPage();
    const setupPage = new DagsPage(page);

    await setupPage.triggerDag(testDagId);
    await setupPage.triggerDag(testDagId);

    const response = await page.request.get(`/api/v2/dags/${testDagId}/dagRuns?limit=1`);
    expect(response.ok()).toBeTruthy();
    const data = await response.json();
    // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment
    const runId = data.dag_runs?.[0]?.dag_run_id;

    if (runId) {
      await page.request.patch(`/api/v2/dags/${testDagId}/dagRuns/${runId}`, {
        data: { state: "failed" },
      });
    }

    await context.close();
  });

  let loginPage: LoginPage;
  let dagsPage: DagsPage;

  test.beforeEach(async ({ page }) => {
    loginPage = new LoginPage(page);
    dagsPage = new DagsPage(page);

    await loginPage.navigateAndLogin(testCredentials.username, testCredentials.password);
    await loginPage.expectLoginSuccess();
  });



  test("verify runs table displays with valid data", async () => {
    await dagsPage.navigateToRunsTab(testDagId);

    // Verify runs table is displayed
    await dagsPage.verifyRunsTabDisplayed();

    // Verify we can see run details
    const runs = await dagsPage.getRunDetails();

    expect(runs.length).toBeGreaterThan(0);
  });

  test("verify run details page navigation", async () => {
    await dagsPage.navigateToRunsTab(testDagId);

    const runs = await dagsPage.getRunDetails();

    expect(runs.length).toBeGreaterThan(0);

    const firstRun = runs[0];

    if (!firstRun) {
      throw new Error("No runs found");
    }

    await dagsPage.clickRun(firstRun.runId);

    // Verify we're on the run details page
    await dagsPage.verifyRunDetailsPage(firstRun.runId);
  });

  test("verify filtering", async () => {
    await dagsPage.navigateToRunsTab(testDagId);

    const runs = await dagsPage.getRunDetails();

    expect(runs.length).toBeGreaterThan(0);

    // We created a failed run in beforeAll, so filter by 'Failed'
    const targetState = "Failed";

    await dagsPage.filterByState(targetState);

    const filteredRuns = await dagsPage.getRunDetails();

    // Verify filtering works - all results should match the target state
    expect(filteredRuns.length).toBeGreaterThan(0);
    expect(filteredRuns.every((run) => run.state === targetState)).toBeTruthy();
  });

  test("verify pagination", async () => {
    await dagsPage.navigateToRunsTab(testDagId);

    const initialRuns = await dagsPage.getRunDetails();

    expect(initialRuns.length).toBeGreaterThan(0);

    const firstRunId = initialRuns[0]?.runId;

    await dagsPage.clickRunPaginationNext();

    const nextPageRuns = await dagsPage.getRunDetails();

    expect(nextPageRuns.length).toBeGreaterThan(0);
    expect(nextPageRuns[0]?.runId).not.toEqual(firstRunId);

    await dagsPage.clickRunPaginationPrev();

    const prevPageRuns = await dagsPage.getRunDetails();

    expect(prevPageRuns[0]?.runId).toEqual(firstRunId);
  });
});
