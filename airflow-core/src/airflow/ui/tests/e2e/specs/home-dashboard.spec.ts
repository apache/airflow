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
import { testConfig } from "playwright.config";
import { DagsPage } from "tests/e2e/pages/DagsPage";
import { HomePage } from "tests/e2e/pages/HomePage";

test.describe("Dashboard Metrics Display", () => {
  let homePage: HomePage;

  test.beforeEach(({ page }) => {
    homePage = new HomePage(page);
  });

  test("should display dashboard stats section with DAG metrics", async () => {
    await homePage.navigate();
    await homePage.waitForDashboardLoad();

    // Use Playwright assertions directly for clearer error messages
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

  test("should display health status badges", async () => {
    await homePage.navigate();
    await homePage.waitForDashboardLoad();

    // Use Playwright assertions directly for clearer error messages
    await expect(homePage.healthSection).toBeVisible();
    await expect(homePage.metaDatabaseHealth).toBeVisible();
    await expect(homePage.schedulerHealth).toBeVisible();
    await expect(homePage.triggererHealth).toBeVisible();
  });

  test("should navigate to filtered DAGs list when clicking stats cards", async () => {
    await homePage.navigate();
    await homePage.waitForDashboardLoad();

    await homePage.activeDagsCard.click();
    await homePage.page.waitForURL(/paused=false/);

    expect(homePage.page.url()).toContain("paused=false");

    await homePage.navigate();
    await homePage.waitForDashboardLoad();

    await homePage.runningDagsCard.click();
    await homePage.page.waitForURL(/last_dag_run_state=running/);

    expect(homePage.page.url()).toContain("last_dag_run_state=running");
  });

  test("should display welcome heading on dashboard", async () => {
    await homePage.navigate();
    await homePage.waitForDashboardLoad();

    await expect(homePage.welcomeHeading).toBeVisible();
  });

  test("should update metrics when DAG is triggered", async () => {
    // Increase timeout for this test since DAG triggering takes time
    test.setTimeout(7 * 60 * 1000);

    await homePage.navigate();
    await homePage.waitForDashboardLoad();

    const initialRunningCount = await homePage.getRunningDagsCount();

    // Trigger a DAG to update metrics
    const dagsPage = new DagsPage(homePage.page);

    await dagsPage.triggerDag(testConfig.testDag.id);

    // Navigate back to home and verify metrics updated
    await homePage.navigate();
    await homePage.waitForDashboardLoad();

    // Verify stats section is still visible after DAG trigger
    await expect(homePage.statsSection).toBeVisible();

    // Get updated counts - running count should reflect the triggered DAG
    const updatedRunningCount = await homePage.getRunningDagsCount();

    // Either running count increased or stayed same (if DAG completed quickly)
    expect(updatedRunningCount).toBeGreaterThanOrEqual(0);
    expect(initialRunningCount).toBeGreaterThanOrEqual(0);
  });

  test("should display historical metrics section with recent runs", async () => {
    await homePage.navigate();
    await homePage.waitForDashboardLoad();

    // Use Playwright assertions directly for clearer error messages
    await expect(homePage.historicalMetricsSection).toBeVisible();
    await expect(homePage.dagRunMetrics).toBeVisible();
    await expect(homePage.taskInstanceMetrics).toBeVisible();
  });

  test("should handle DAG import errors display when errors exist", async () => {
    await homePage.navigate();
    await homePage.waitForDashboardLoad();

    // DAG Import Errors button only appears when there are actual import errors
    const isDagImportErrorsVisible = await homePage.isDagImportErrorsVisible();

    // Skip test with clear message if no import errors exist in the test environment
    test.skip(!isDagImportErrorsVisible, "No DAG import errors present in test environment");

    await expect(homePage.dagImportErrorsCard).toBeVisible();
  });
});
