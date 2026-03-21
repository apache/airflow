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
import { DagRunsPage } from "tests/e2e/pages/DagRunsPage";
import {
  apiCreateDagRun,
  apiSetDagRunState,
  uniqueRunId,
  waitForDagReady,
} from "tests/e2e/utils/test-helpers";

test.describe("DAG Runs Page", () => {
  test.setTimeout(60_000);

  let dagRunsPage: DagRunsPage;
  const testDagId1 = testConfig.testDag.id;
  const testDagId2 = "example_python_operator";

  test.beforeAll(async ({ browser }) => {
    const context = await browser.newContext({ storageState: AUTH_FILE });
    const page = await context.newPage();

    // Wait for both DAGs to be parsed before making API calls
    await waitForDagReady(page, testDagId1);
    await waitForDagReady(page, testDagId2);

    const timestamp = Date.now();

    // Create first DAG run and mark as failed (unique ID per worker)
    const runId1 = uniqueRunId("dagrun_failed");
    const logicalDate1 = new Date(timestamp).toISOString();

    await apiCreateDagRun(page, testDagId1, {
      dag_run_id: runId1,
      logical_date: logicalDate1,
    });
    await apiSetDagRunState(page, { dagId: testDagId1, runId: runId1, state: "failed" });

    // Create second DAG run and mark as success
    const runId2 = uniqueRunId("dagrun_success");
    const logicalDate2 = new Date(timestamp + 60_000).toISOString();

    await apiCreateDagRun(page, testDagId1, {
      dag_run_id: runId2,
      logical_date: logicalDate2,
    });
    await apiSetDagRunState(page, { dagId: testDagId1, runId: runId2, state: "success" });

    // Create a run for a different DAG (for DAG ID filtering test)
    const runId3 = uniqueRunId("dagrun_other");
    const logicalDate3 = new Date(timestamp + 120_000).toISOString();

    await apiCreateDagRun(page, testDagId2, {
      dag_run_id: runId3,
      logical_date: logicalDate3,
    });

    await context.close();
  });

  test.beforeEach(({ page }) => {
    dagRunsPage = new DagRunsPage(page);
  });

  test("verify DAG runs table displays data", async () => {
    await dagRunsPage.navigate();
    await dagRunsPage.verifyDagRunsExist();
  });

  test("verify run details display correctly", async () => {
    await dagRunsPage.navigate();
    await dagRunsPage.verifyRunDetailsDisplay();
  });

  test("verify filtering by failed state", async () => {
    await dagRunsPage.navigate();
    await dagRunsPage.verifyStateFiltering("Failed");
  });

  test("verify filtering by success state", async () => {
    await dagRunsPage.navigate();
    await dagRunsPage.verifyStateFiltering("Success");
  });

  test("verify filtering by DAG ID", async () => {
    await dagRunsPage.navigate();
    await dagRunsPage.verifyDagIdFiltering(testDagId1);
  });
});
