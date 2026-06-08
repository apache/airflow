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

import { COLLAPSED_UI_ALERTS_KEY } from "src/constants/localStorage";

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

test.describe("Dashboard Alerts Accordion", () => {
  test(`should not write ${COLLAPSED_UI_ALERTS_KEY} to localStorage when no alerts are configured`, async ({
    homePage,
  }) => {
    await homePage.mockDashboardAlerts([]);

    await homePage.navigate();
    await homePage.page.evaluate((key) => localStorage.removeItem(key), COLLAPSED_UI_ALERTS_KEY);
    await homePage.navigate();
    await homePage.waitForDashboardLoad();

    // Key must remain null — the accordion is never rendered so handleAccordionChange never fires
    const stored = await homePage.page.evaluate((key) => localStorage.getItem(key), COLLAPSED_UI_ALERTS_KEY);

    expect(stored).toBeNull();
  });

  test("should show 3 alerts, collapse to 1, and persist collapsed state across navigation", async ({
    homePage,
  }) => {
    const threeAlerts = [
      { category: "info" as const, text: "Alert one" },
      { category: "warning" as const, text: "Alert two" },
      { category: "error" as const, text: "Alert three" },
    ];

    await homePage.mockDashboardAlerts(threeAlerts);

    await homePage.navigate();
    await homePage.page.evaluate((key) => localStorage.removeItem(key), COLLAPSED_UI_ALERTS_KEY);
    await homePage.navigate();
    await homePage.waitForDashboardLoad();

    // All 3 alerts visible: trigger (index 0) + 2 content items (index 1, 2)
    await expect(homePage.page.getByText("Alert one")).toBeVisible();
    await expect(homePage.page.getByText("Alert two")).toBeVisible();
    await expect(homePage.page.getByText("Alert three")).toBeVisible();
    expect(await homePage.getAlertsCollapsedFromStorage()).toBe(false);

    await homePage.alertsAccordionTrigger.click();
    await expect(homePage.alertsAccordionContent).toHaveAttribute("data-state", "closed");
    await expect(homePage.page.getByText("Alert one")).toBeVisible();
    await expect(homePage.page.getByText("Alert two")).not.toBeVisible();
    await expect(homePage.page.getByText("Alert three")).not.toBeVisible();
    expect(await homePage.getAlertsCollapsedFromStorage()).toBe(true);

    await expect(homePage.alertsShowMoreButton).toBeVisible();
    await expect(homePage.alertsShowMoreButton).toHaveText("+2 more alerts");

    // Reload to confirm the collapsed state is restored from localStorage
    await homePage.navigate();
    await homePage.waitForDashboardLoad();
    await expect(homePage.alertsAccordionContent).toHaveAttribute("data-state", "closed");
    await expect(homePage.page.getByText("Alert one")).toBeVisible();
    await expect(homePage.page.getByText("Alert two")).not.toBeVisible();
    await expect(homePage.page.getByText("Alert three")).not.toBeVisible();
    expect(await homePage.getAlertsCollapsedFromStorage()).toBe(true);

    await homePage.alertsShowMoreButton.click();
    await expect(homePage.alertsAccordionContent).toHaveAttribute("data-state", "open");
    await expect(homePage.page.getByText("Alert two")).toBeVisible();
    await expect(homePage.page.getByText("Alert three")).toBeVisible();
    await expect(homePage.alertsShowMoreButton).not.toBeVisible();
    expect(await homePage.getAlertsCollapsedFromStorage()).toBe(false);
  });
});

test.describe("Dashboard Single Alert", () => {
  const singleAlert = [{ category: "info" as const, text: "Only alert" }];

  test.beforeEach(async ({ homePage }) => {
    await homePage.mockDashboardAlerts(singleAlert);

    await homePage.navigate();
    await homePage.page.evaluate((key) => localStorage.removeItem(key), COLLAPSED_UI_ALERTS_KEY);
    await homePage.navigate();
    await homePage.waitForDashboardLoad();
  });

  test("renders the alert without an accordion trigger or show-more button", async ({ homePage }) => {
    await expect(homePage.page.getByText("Only alert")).toBeVisible();

    await expect(homePage.alertsAccordionTrigger).toHaveCount(0);
    await expect(homePage.alertsShowMoreButton).toHaveCount(0);
  });

  test("does not persist a collapsed state for a single alert", async ({ homePage }) => {
    await expect(homePage.page.getByText("Only alert")).toBeVisible();

    // No trigger means handleAccordionChange never fires, so the key stays untouched.
    const stored = await homePage.page.evaluate((key) => localStorage.getItem(key), COLLAPSED_UI_ALERTS_KEY);

    expect(stored).toBeNull();
  });
});

test.describe("Dashboard Alert See More / See Less", () => {
  // Many markdown paragraphs guarantee the alert renders past the line limit at any width.
  const longText = [
    "Long alert start",
    ...Array.from({ length: 10 }, (_, index) => `Line ${index + 2}`),
    "Long alert end",
  ].join("\n\n");
  const alerts = [
    { category: "info" as const, text: longText },
    { category: "warning" as const, text: "Short alert text" },
  ];

  test.beforeEach(async ({ homePage }) => {
    await homePage.mockDashboardAlerts(alerts);

    await homePage.navigate();
    await homePage.page.evaluate((key) => localStorage.removeItem(key), COLLAPSED_UI_ALERTS_KEY);
    await homePage.navigate();
    await homePage.waitForDashboardLoad();
  });

  test("collapses only the tall alert and clamps its height until expanded", async ({ homePage }) => {
    await expect(homePage.alertSeeMoreButton).toHaveCount(1);
    await expect(homePage.alertSeeMoreButton).toBeVisible();
    await expect(homePage.alertSeeLessButton).toHaveCount(0);

    // The tall alert is the accordion trigger; its height reflects the clamp.
    const collapsedHeight = (await homePage.alertsAccordionTrigger.boundingBox())?.height ?? 0;

    await homePage.alertSeeMoreButton.click();
    await expect(homePage.alertSeeLessButton).toBeVisible();
    await expect(homePage.alertSeeMoreButton).toHaveCount(0);

    const expandedHeight = (await homePage.alertsAccordionTrigger.boundingBox())?.height ?? 0;

    expect(expandedHeight).toBeGreaterThan(collapsedHeight);

    await homePage.alertSeeLessButton.click();
    await expect(homePage.alertSeeMoreButton).toBeVisible();
    expect((await homePage.alertsAccordionTrigger.boundingBox())?.height ?? 0).toBeLessThan(expandedHeight);
  });

  test("expanding the first alert does not toggle the surrounding accordion", async ({ homePage }) => {
    // The tall alert is the accordion trigger (index 0); its See more button must not
    // bubble the click up and collapse the accordion, so the index-1 alert stays visible.
    await expect(homePage.page.getByText("Short alert text")).toBeVisible();

    await homePage.alertSeeMoreButton.click();

    await expect(homePage.alertSeeLessButton).toBeVisible();
    await expect(homePage.page.getByText("Short alert text")).toBeVisible();
    expect(await homePage.getAlertsCollapsedFromStorage()).toBe(false);
  });
});
