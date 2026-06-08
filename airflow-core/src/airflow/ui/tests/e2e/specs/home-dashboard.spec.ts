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
import { expect } from "tests/e2e/fixtures";
import { test } from "tests/e2e/fixtures/dashboard-data";

test.describe("Dashboard Metrics Display", () => {
  test("should display dashboard stats section with Dag metrics", async ({ homePage }) => {
    await homePage.navigate();
    await homePage.waitForDashboardLoad();

    await expect(homePage.statsSection).toBeVisible();

    await expect(homePage.activeDagsCard).toBeVisible();
    const activeDagsCount = await homePage.getActiveDagsCount();

    expect(activeDagsCount).toBeGreaterThanOrEqual(0);

    await expect(homePage.runningDagsCard).toBeVisible();
    const runningDagsCount = await homePage.getRunningDagsCount();

    expect(runningDagsCount).toBeGreaterThanOrEqual(0);

    await expect(homePage.failedDagsCard).toBeVisible();
    const failedDagsCount = await homePage.getFailedDagsCount();

    expect(failedDagsCount).toBeGreaterThanOrEqual(0);
  });

  test("should display health status badges", async ({ homePage }) => {
    await homePage.navigate();
    await homePage.waitForDashboardLoad();

    await expect(homePage.healthSection).toBeVisible();
    await expect(homePage.metaDatabaseHealth).toBeVisible();
    await expect(homePage.schedulerHealth).toBeVisible();
    await expect(homePage.triggererHealth).toBeVisible();
  });

  test("should navigate to filtered Dags list when clicking stats cards", async ({ homePage }) => {
    await homePage.navigate();
    await homePage.waitForDashboardLoad();

    await homePage.activeDagsCard.click();
    await expect(homePage.page).toHaveURL(/paused=false/);

    await homePage.navigate();
    await homePage.waitForDashboardLoad();

    await homePage.runningDagsCard.click();
    await expect(homePage.page).toHaveURL(/last_dag_run_state=running/);
  });

  test("should display welcome heading on dashboard", async ({ homePage }) => {
    await homePage.navigate();
    await homePage.waitForDashboardLoad();

    await expect(homePage.welcomeHeading).toBeVisible();
  });

  test("should update metrics when Dag is triggered", async ({ dagRunCleanup, dagsPage, homePage }) => {
    test.slow();

    await homePage.navigate();
    await homePage.waitForDashboardLoad();

    const dagRunId = await dagsPage.triggerDag(testConfig.testDag.id);

    if (dagRunId !== null) {
      dagRunCleanup.track(dagRunId);
    }

    await homePage.navigate();
    await homePage.waitForDashboardLoad();

    await expect(homePage.statsSection).toBeVisible();
    await expect(homePage.activeDagsCard).toBeVisible();
    await expect(homePage.runningDagsCard).toBeVisible();
  });

  test("should display historical metrics section with recent runs", async ({ homePage }) => {
    await homePage.navigate();
    await homePage.waitForDashboardLoad();

    await expect(homePage.historicalMetricsSection).toBeVisible();
    await expect(homePage.dagRunMetrics).toBeVisible();
    await expect(homePage.taskInstanceMetrics).toBeVisible();
  });

  test("should handle Dag import errors display when errors exist", async ({ homePage }) => {
    await homePage.navigate();
    await homePage.waitForDashboardLoad();

    const isDagImportErrorsVisible = await homePage.isDagImportErrorsVisible();

    test.skip(!isDagImportErrorsVisible, "No Dag import errors present in test environment");

    await expect(homePage.dagImportErrorsCard).toBeVisible();
  });
});
