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
import { test, expect } from "@playwright/test";
import { AUTH_FILE, testConfig } from "playwright.config";
import { DagRunsPage } from "tests/e2e/pages/DagRunsPage";

test.describe("DAG Runs Page", () => {
  test.setTimeout(60_000);

  let dagRunsPage: DagRunsPage;
  const testDagId1 = testConfig.testDag.id;
  const testDagId2 = "example_python_operator";
  let failedRunId: string;
  let successRunId: string;

  test.beforeAll(async ({ browser }) => {
    const context = await browser.newContext({ storageState: AUTH_FILE });
    const page = await context.newPage();
    const baseUrl = process.env.AIRFLOW_UI_BASE_URL ?? "http://localhost:8080";

    const timestamp = Date.now();

    // Trigger first DAG run
    const runId1 = `test_run_failed_${timestamp}`;
    const logicalDate1 = new Date(timestamp).toISOString();
    const triggerResponse1 = await page.request.post(`${baseUrl}/api/v2/dags/${testDagId1}/dagRuns`, {
      data: JSON.stringify({
        dag_run_id: runId1,
        logical_date: logicalDate1,
      }),
      headers: {
        "Content-Type": "application/json",
      },
    });

    expect(triggerResponse1.ok()).toBeTruthy();
    const runData1 = (await triggerResponse1.json()) as { dag_run_id: string };

    failedRunId = runData1.dag_run_id;

    // Mark the first run as failed
    const patchResponse = await page.request.patch(
      `${baseUrl}/api/v2/dags/${testDagId1}/dagRuns/${failedRunId}`,
      {
        data: JSON.stringify({ state: "failed" }),
        headers: {
          "Content-Type": "application/json",
        },
      },
    );

    expect(patchResponse.ok()).toBeTruthy();

    // Trigger second DAG run for success state
    const runId2 = `test_run_success_${timestamp}`;
    const logicalDate2 = new Date(timestamp + 60_000).toISOString();
    const triggerResponse2 = await page.request.post(`${baseUrl}/api/v2/dags/${testDagId1}/dagRuns`, {
      data: JSON.stringify({
        dag_run_id: runId2,
        logical_date: logicalDate2,
      }),
      headers: {
        "Content-Type": "application/json",
      },
    });

    expect(triggerResponse2.ok()).toBeTruthy();
    const runData2 = (await triggerResponse2.json()) as { dag_run_id: string };

    successRunId = runData2.dag_run_id;

    // Mark the second run as success
    const patchResponse2 = await page.request.patch(
      `${baseUrl}/api/v2/dags/${testDagId1}/dagRuns/${successRunId}`,
      {
        data: JSON.stringify({ state: "success" }),
        headers: {
          "Content-Type": "application/json",
        },
      },
    );

    expect(patchResponse2.ok()).toBeTruthy();

    // Trigger a run for a different DAG
    const runId3 = `test_run_other_dag_${timestamp}`;
    const logicalDate3 = new Date(timestamp + 120_000).toISOString();

    await page.request.post(`${baseUrl}/api/v2/dags/${testDagId2}/dagRuns`, {
      data: JSON.stringify({
        dag_run_id: runId3,
        logical_date: logicalDate3,
      }),
      headers: {
        "Content-Type": "application/json",
      },
    });

    await context.close();
  });

  test.beforeEach(({ page }) => {
    dagRunsPage = new DagRunsPage(page);
  });

  test("should display the DAG runs table with data", async () => {
    await dagRunsPage.navigate();
    await dagRunsPage.verifyDagRunsExist();
  });

  test("should display run details: run ID, state, and dates", async () => {
    await dagRunsPage.navigate();
    await dagRunsPage.verifyRunDetailsDisplay();
  });

  test("should filter by failed state correctly", async () => {
    await dagRunsPage.navigate();
    await dagRunsPage.verifyStateFiltering("Failed");
  });

  test("should filter by success state correctly", async () => {
    await dagRunsPage.navigate();
    await dagRunsPage.verifyStateFiltering("Success");
  });

  test("should filter by DAG ID correctly", async () => {
    await dagRunsPage.navigate();
    await dagRunsPage.verifyDagIdFiltering(testDagId1);
  });

  test("should support pagination with offset and limit", async () => {
    await dagRunsPage.verifyPagination(3);
  });
});
