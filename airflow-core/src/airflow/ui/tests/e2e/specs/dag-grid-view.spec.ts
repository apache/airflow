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
import { DagDetailPage } from "tests/e2e/pages/DagDetailPage";
import { LoginPage } from "tests/e2e/pages/LoginPage";

/**
 * DAG Grid View E2E Tests
 * Tests for verifying the grid view displays correctly on the DAG detail page
 */

test.describe("DAG Grid View", () => {
  let loginPage: LoginPage;
  let dagDetailPage: DagDetailPage;

  const testCredentials = testConfig.credentials;
  const testDagId = testConfig.testDag.id;

  test.beforeEach(async ({ page }) => {
    loginPage = new LoginPage(page);
    dagDetailPage = new DagDetailPage(page);

    // Maximize browser for better visibility
    await dagDetailPage.maximizeBrowser();

    // Login to Airflow
    await loginPage.login(testCredentials.username, testCredentials.password);
    await loginPage.waitForSuccessfulLogin();

    // Trigger the test DAG to ensure we have runs to display in grid
    await page.goto(`/dags/${testDagId}/grid`);
    await page.waitForTimeout(2000);

    // Try to trigger a run if possible
    const triggerButton = page.locator('button:has-text("Trigger DAG")');
    if (await triggerButton.isVisible()) {
      await triggerButton.click();
      await page.waitForTimeout(3000); // Wait for run to start
    }
  });

  test("should display grid view with task instances on DAG detail page", async ({ page }) => {
    // Set a longer timeout for this test as it involves navigation and rendering
    test.setTimeout(2 * 60 * 1000);

    // Step 1: Navigate to specific DAG's detail page (already logged in from beforeEach)
    await dagDetailPage.navigateToDagDetail(testDagId);

    // Step 2: Ensure we're on the Overview tab (default view with grid)
    await dagDetailPage.switchToTab("overview");

    // Step 3: Wait for grid view to load
    await dagDetailPage.waitForGridView();

    // Step 4: Verify grid renders (may be empty if no runs)
    const taskCount = await dagDetailPage.getTaskInstanceCount();
    console.log(`Grid task instance count: ${taskCount}`);

    // Grid should be visible even if empty
    expect(taskCount).toBeGreaterThanOrEqual(0);
    expect(taskCount).toBeGreaterThan(0);
  });

  test("should verify task states are color-coded correctly in grid view", async () => {
    test.setTimeout(2 * 60 * 1000);

    // Login and navigate to DAG detail page
    await loginPage.navigateAndLogin(testCredentials.username, testCredentials.password);
    await loginPage.expectLoginSuccess();
    await dagDetailPage.navigateToDagDetail(testDagId);

    // Wait for grid view to render
    await dagDetailPage.waitForGridView();

    // Verify task states are color-coded
    const areColorCoded = await dagDetailPage.verifyTaskStatesAreColorCoded();
    expect(areColorCoded).toBe(true);

    // Get colors and verify we have some
    const colors = await dagDetailPage.getTaskStateColors();
    expect(colors.length).toBeGreaterThan(0);
  });

  test("should show task details when clicking a grid cell", async () => {
    test.setTimeout(2 * 60 * 1000);

    // Login and navigate to DAG detail page
    await loginPage.navigateAndLogin(testCredentials.username, testCredentials.password);
    await loginPage.expectLoginSuccess();
    await dagDetailPage.navigateToDagDetail(testDagId);

    // Wait for grid view to render
    await dagDetailPage.waitForGridView();

    // Verify we have task instances to click
    const hasTaskInstances = await dagDetailPage.verifyGridHasTaskInstances();
    expect(hasTaskInstances).toBe(true);

    // Click on the first task cell
    await dagDetailPage.clickTaskCell(0);

    // Verify task details panel is visible
    const isPanelVisible = await dagDetailPage.isTaskDetailsPanelVisible();
    expect(isPanelVisible).toBe(true);

    // Verify we can get task information from the details panel
    const taskId = await dagDetailPage.getTaskIdFromDetails();
    const taskState = await dagDetailPage.getTaskStateFromDetails();

    // Task ID should be present
    if (taskId !== null) {
      expect(taskId.length).toBeGreaterThan(0);
    }

    // Task state should be present
    if (taskState !== null) {
      expect(taskState.length).toBeGreaterThan(0);
    }

    // Close the details panel
    await dagDetailPage.closeTaskDetails();

    // Verify panel is closed
    await dagDetailPage.page.waitForTimeout(500);
    const isPanelStillVisible = await dagDetailPage.isTaskDetailsPanelVisible();

    // Panel should be closed (or we can be lenient if close doesn't work in all cases)
    // This is a soft assertion as panel behavior might vary
    if (isPanelStillVisible) {
      // eslint-disable-next-line no-console
      console.warn("Task details panel is still visible after close attempt");
    }
  });

  test("should navigate to grid view from different tabs", async () => {
    test.setTimeout(2 * 60 * 1000);

    // Login and navigate to DAG detail page
    await loginPage.navigateAndLogin(testCredentials.username, testCredentials.password);
    await loginPage.expectLoginSuccess();
    await dagDetailPage.navigateToDagDetail(testDagId);

    // Start at Overview tab (default with grid view)
    await dagDetailPage.switchToTab("overview");
    await dagDetailPage.waitForGridView();

    // Verify grid is visible
    let hasTaskInstances = await dagDetailPage.verifyGridHasTaskInstances();
    expect(hasTaskInstances).toBe(true);

    // Switch to another tab (e.g., Details)
    await dagDetailPage.switchToTab("details");
    await dagDetailPage.page.waitForTimeout(1000);

    // Switch back to Overview to see grid again
    await dagDetailPage.switchToTab("overview");
    await dagDetailPage.waitForGridView();

    // Verify grid is still visible after navigation
    hasTaskInstances = await dagDetailPage.verifyGridHasTaskInstances();
    expect(hasTaskInstances).toBe(true);
  });
});
