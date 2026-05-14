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
import { testConfig } from "playwright.config";
import { expect, test } from "tests/e2e/fixtures";
import { apiDeleteDagRun, waitForDagRunStatus } from "tests/e2e/utils/test-helpers";

test.describe("Dag Trigger Workflow", () => {
  const testDagId = testConfig.testDag.id;

  test("should successfully trigger a Dag run", async ({
    authenticatedRequest,
    dagReady: _ready,
    dagsPage,
    page,
  }) => {
    test.slow();

    const dagRunId = await dagsPage.triggerDag(testDagId);

    expect(dagRunId).toBeTruthy();

    if (dagRunId !== null) {
      await waitForDagRunStatus(authenticatedRequest, {
        dagId: testDagId,
        expectedState: "success",
        runId: dagRunId,
        timeout: 120_000,
      });

      await page.goto(`/dags/${testDagId}/runs/${dagRunId}`);
      const stateBadge = page.getByTestId("state-badge").first();

      await expect(stateBadge).toContainText("Success", { timeout: 30_000 });

      await apiDeleteDagRun(authenticatedRequest, testDagId, dagRunId).catch(() => undefined);
    }
  });
});

test.describe("Dag Details Tab", () => {
  const testDagId = testConfig.testDag.id;

  test("should successfully verify details tab", async ({ dagReady: _ready, dagsPage }) => {
    test.slow();
    await dagsPage.navigateToDagDetails(testDagId);
  });
});

test.describe("Dags List Display", () => {
  // dagReady is triggered once per worker via beforeEach.
  // eslint-disable-next-line @typescript-eslint/no-empty-function -- triggers worker-scoped data fixture
  test.beforeEach(async ({ dagReady: _ready }) => {});

  test("should display Dags list after successful login", async ({ dagsPage }) => {
    test.slow();
    await dagsPage.navigate();
    await dagsPage.waitForDagList();

    const dagsCount = await dagsPage.getDagsCount();

    expect(dagsCount).toBeGreaterThan(0);
  });

  test("should display Dag links correctly", async ({ dagsPage }) => {
    test.slow();
    await dagsPage.navigate();
    await dagsPage.waitForDagList();

    const dagLinks = await dagsPage.getDagLinks();

    expect(dagLinks.length).toBeGreaterThan(0);

    for (const link of dagLinks) {
      expect(link).toMatch(/\/dags\/.+/);
    }
  });

  test("should display test Dag in the list", async ({ dagsPage }) => {
    test.slow();
    const testDagId = testConfig.testDag.id;

    await dagsPage.navigate();
    await dagsPage.waitForDagList();
    await expect(dagsPage.getDagLink(testDagId)).toBeVisible();
  });
});

test.describe("Dags View Toggle", () => {
  test("should toggle between card view and table view", async ({ dagsPage }) => {
    test.slow();
    await dagsPage.navigate();
    await dagsPage.waitForDagList();

    await dagsPage.switchToCardView();
    await dagsPage.waitForCardView();

    const cardViewDagsCount = await dagsPage.getDagsCount();

    expect(cardViewDagsCount).toBeGreaterThan(0);

    await dagsPage.switchToTableView();
    await dagsPage.waitForTableView();

    const tableViewDagsCount = await dagsPage.getDagsCount();

    expect(tableViewDagsCount).toBeGreaterThan(0);
  });
});

test.describe("Dags Search", () => {
  const testDagId = testConfig.testDag.id;

  test("should search for a Dag by name", async ({ dagsPage }) => {
    test.slow();
    await dagsPage.navigate();
    await dagsPage.waitForDagList();

    const initialCount = await dagsPage.getDagsCount();

    expect(initialCount).toBeGreaterThan(0);

    await dagsPage.searchDag(testDagId);
    await expect(dagsPage.getDagLink(testDagId)).toBeVisible();
    await dagsPage.clearSearch();

    await dagsPage.waitForDagList();

    await expect
      .poll(async () => dagsPage.getDagsCount(), {
        message: "Waiting for DAGs count to restore after clearing search",
        timeout: 10_000,
      })
      .toBe(initialCount);
  });
});

test.describe("Dags Status Filtering", () => {
  test("should display status filter buttons", async ({ dagsPage }) => {
    test.slow();
    await dagsPage.navigate();
    await dagsPage.waitForDagList();

    await expect(dagsPage.successFilter).toBeVisible();
    await expect(dagsPage.failedFilter).toBeVisible();
    await expect(dagsPage.runningFilter).toBeVisible();
    await expect(dagsPage.queuedFilter).toBeVisible();

    await dagsPage.filterByStatus("success");
    await dagsPage.waitForDagList();

    await dagsPage.filterByStatus("failed");
    await dagsPage.waitForDagList();
  });
});
