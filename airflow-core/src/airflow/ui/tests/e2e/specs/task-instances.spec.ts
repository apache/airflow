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
import { TaskInstancesPage } from "tests/e2e/pages/TaskInstancesPage";

test.describe("Task Instances Page", () => {
  test.setTimeout(60_000);

  let taskInstancesPage: TaskInstancesPage;
  const testDagId = testConfig.testDag.id;

  test.beforeAll(async ({ browser }) => {
    const context = await browser.newContext({ storageState: AUTH_FILE });
    const page = await context.newPage();
    const baseUrl = process.env.AIRFLOW_UI_BASE_URL ?? "http://localhost:8080";
    const timestamp = Date.now();

    // Create first DAG run for success state
    const runId1 = `test_ti_success_${timestamp}`;
    const logicalDate1 = new Date(timestamp).toISOString();
    const triggerResponse1 = await page.request.post(`${baseUrl}/api/v2/dags/${testDagId}/dagRuns`, {
      data: JSON.stringify({
        dag_run_id: runId1,
        logical_date: logicalDate1,
      }),
      headers: {
        "Content-Type": "application/json",
      },
    });

    expect(triggerResponse1.ok()).toBeTruthy();

    // Get all task instances for the first run
    const tasksResponse1 = await page.request.get(
      `${baseUrl}/api/v2/dags/${testDagId}/dagRuns/${runId1}/taskInstances`,
    );

    expect(tasksResponse1.ok()).toBeTruthy();

    const tasksData1 = (await tasksResponse1.json()) as {
      task_instances: Array<{ task_id: string }>;
    };

    // Mark all tasks as success
    for (const task of tasksData1.task_instances) {
      const patchResponse = await page.request.patch(
        `${baseUrl}/api/v2/dags/${testDagId}/dagRuns/${runId1}/taskInstances/${task.task_id}`,
        {
          data: JSON.stringify({ new_state: "success" }),
          headers: { "Content-Type": "application/json" },
        },
      );

      expect(patchResponse.ok()).toBeTruthy();
    }

    // Create second DAG run for failed state
    const runId2 = `test_ti_failed_${timestamp}`;
    const logicalDate2 = new Date(timestamp + 60_000).toISOString();
    const triggerResponse2 = await page.request.post(`${baseUrl}/api/v2/dags/${testDagId}/dagRuns`, {
      data: JSON.stringify({
        dag_run_id: runId2,
        logical_date: logicalDate2,
      }),
      headers: {
        "Content-Type": "application/json",
      },
    });

    expect(triggerResponse2.ok()).toBeTruthy();

    // Get all task instances for the second run
    const tasksResponse2 = await page.request.get(
      `${baseUrl}/api/v2/dags/${testDagId}/dagRuns/${runId2}/taskInstances`,
    );

    expect(tasksResponse2.ok()).toBeTruthy();

    const tasksData2 = (await tasksResponse2.json()) as {
      task_instances: Array<{ task_id: string }>;
    };

    // Mark all tasks as failed
    for (const task of tasksData2.task_instances) {
      const patchResponse = await page.request.patch(
        `${baseUrl}/api/v2/dags/${testDagId}/dagRuns/${runId2}/taskInstances/${task.task_id}`,
        {
          data: JSON.stringify({ new_state: "failed" }),
          headers: { "Content-Type": "application/json" },
        },
      );

      expect(patchResponse.ok()).toBeTruthy();
    }

    await context.close();
  });

  test.beforeEach(({ page }) => {
    taskInstancesPage = new TaskInstancesPage(page);
  });

  test("verify task instances table displays data", async () => {
    await taskInstancesPage.navigate();
    await taskInstancesPage.verifyTaskInstancesExist();
  });

  test("verify task details display correctly", async () => {
    await taskInstancesPage.navigate();
    await taskInstancesPage.verifyTaskDetailsDisplay();
  });

  test("verify filtering by failed state", async () => {
    await taskInstancesPage.navigate();
    await taskInstancesPage.verifyStateFiltering("Failed");
  });

  test("verify filtering by success state", async () => {
    await taskInstancesPage.navigate();
    await taskInstancesPage.verifyStateFiltering("Success");
  });

  test("verify pagination with offset and limit", async () => {
    await taskInstancesPage.navigate();

    await expect(taskInstancesPage.paginationNextButton).toBeVisible();
    await expect(taskInstancesPage.paginationPrevButton).toBeVisible();

    const initialTaskInstanceIds = await taskInstancesPage.getTaskInstanceIds();

    expect(initialTaskInstanceIds.length).toBeGreaterThan(0);

    await taskInstancesPage.clickNextPage();

    const taskInstanceIdsAfterNext = await taskInstancesPage.getTaskInstanceIds();

    expect(taskInstanceIdsAfterNext.length).toBeGreaterThan(0);
    expect(taskInstanceIdsAfterNext).not.toEqual(initialTaskInstanceIds);

    await taskInstancesPage.clickPrevPage();

    const taskInstanceIdsAfterPrev = await taskInstancesPage.getTaskInstanceIds();

    expect(taskInstanceIdsAfterPrev).toEqual(initialTaskInstanceIds);
  });
});
