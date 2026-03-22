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
import {
  apiCreateDagRun,
  apiSetDagRunState,
  uniqueRunId,
  waitForDagReady,
} from "tests/e2e/utils/test-helpers";

test.describe("DAG Runs Tab", () => {
  test.setTimeout(60_000);

  let dagRunsTabPage: DagRunsTabPage;
  const testDagId = testConfig.testDag.id;

  // API-based setup replaces UI-based triggerDagRun/markRunAs to avoid
  // dialog race conditions. UI trigger coverage is maintained in dags-list.spec.ts.
  test.beforeAll(async ({ browser }) => {
    test.setTimeout(120_000);
    const context = await browser.newContext({ storageState: AUTH_FILE });
    const page = await context.newPage();

    await waitForDagReady(page, testDagId);

    const timestamp = Date.now();

    // Create a "success" run
    const runId1 = uniqueRunId("runtab_success");

    await apiCreateDagRun(page, testDagId, {
      dag_run_id: runId1,
      logical_date: new Date(timestamp).toISOString(),
    });
    await apiSetDagRunState(page, { dagId: testDagId, runId: runId1, state: "success" });

    // Create a "failed" run
    const runId2 = uniqueRunId("runtab_failed");

    await apiCreateDagRun(page, testDagId, {
      dag_run_id: runId2,
      logical_date: new Date(timestamp + 60_000).toISOString(),
    });
    await apiSetDagRunState(page, { dagId: testDagId, runId: runId2, state: "failed" });

    await context.close();
  });

  test.beforeEach(({ page }) => {
    dagRunsTabPage = new DagRunsTabPage(page);
  });

  test("navigate to DAG detail page and click Runs tab", async () => {
    await dagRunsTabPage.navigateToDag(testDagId);
    await dagRunsTabPage.clickRunsTab();

    await expect(dagRunsTabPage.page).toHaveURL(/.*\/dags\/[^/]+\/runs/);
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
    await dagRunsTabPage.searchByRunIdPattern("runtab");
    await dagRunsTabPage.verifySearchResults("runtab");
  });
});
