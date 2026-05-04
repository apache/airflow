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
import { expect, type Locator, type Page } from "@playwright/test";
import { BasePage } from "tests/e2e/pages/BasePage";

/**
 * Home/Dashboard Page Object
 */
export class HomePage extends BasePage {
  public static get homeUrl(): string {
    return "/";
  }

  public readonly activeDagsCard: Locator;
  public readonly dagImportErrorsCard: Locator;
  public readonly dagProcessorHealth: Locator;
  public readonly dagRunMetrics: Locator;
  public readonly failedDagsCard: Locator;

  public readonly healthSection: Locator;
  public readonly historicalMetricsSection: Locator;
  public readonly metaDatabaseHealth: Locator;
  public readonly poolSummarySection: Locator;
  public readonly runningDagsCard: Locator;

  public readonly schedulerHealth: Locator;

  public readonly statsSection: Locator;
  public readonly taskInstanceMetrics: Locator;
  public readonly triggererHealth: Locator;

  public constructor(page: Page) {
    super(page);

    // Stats cards - using link role with accessible name. The card text includes the label
    // (e.g. "Failed", "Running", "Active") so getByRole("link") with name is reliable.
    // If the accessible name only renders as a number, fall back to href-based selectors.
    this.failedDagsCard = page.getByRole("link", { name: /failed/i });
    this.runningDagsCard = page.getByRole("link", { name: /running/i });
    this.activeDagsCard = page.getByRole("link", { name: /active/i });
    this.dagImportErrorsCard = page.getByRole("button", { name: "Dag Import Errors" });

    // Navigate to parent via ".." since there are no ARIA landmark/region roles on these sections.
    this.statsSection = page.getByRole("heading", { name: "Stats" }).locator("..");
    this.healthSection = page.getByRole("heading", { name: "Health" }).locator("..");
    this.metaDatabaseHealth = page.getByText("Metadatabase").first();
    this.schedulerHealth = page.getByText("Scheduler").first();
    this.triggererHealth = page.getByText("Triggerer").first();
    this.dagProcessorHealth = page.getByText("Dag Processor").first();

    this.poolSummarySection = page.getByRole("heading", { name: "Pool Summary" }).locator("..");

    this.historicalMetricsSection = page.getByRole("heading", { name: "History" }).locator("..");
    this.dagRunMetrics = page.getByRole("heading", { name: /dag run/i }).first();
    this.taskInstanceMetrics = page.getByRole("heading", { name: /task instance/i }).first();
  }

  /**
   * Get Active Dags count
   */
  public async getActiveDagsCount(): Promise<number> {
    return this.getStatsCardCount(this.activeDagsCard);
  }

  /**
   * Get Failed Dags count
   */
  public async getFailedDagsCount(): Promise<number> {
    return this.getStatsCardCount(this.failedDagsCard);
  }

  /**
   * Get Running Dags count
   */
  public async getRunningDagsCount(): Promise<number> {
    return this.getStatsCardCount(this.runningDagsCard);
  }

  /**
   * Check if Dag Import Errors are displayed (only visible when errors exist)
   */
  public async isDagImportErrorsVisible(): Promise<boolean> {
    try {
      return await this.dagImportErrorsCard.isVisible();
    } catch {
      return false;
    }
  }

  /**
   * Navigate to Home/Dashboard page
   */
  public async navigate(): Promise<void> {
    await expect(async () => {
      await this.navigateTo(HomePage.homeUrl);
      await expect(this.welcomeHeading).toBeVisible();
    }).toPass({ intervals: [2000], timeout: 60_000 });
  }

  /**
   * Wait for dashboard to fully load including stats data
   */
  public async waitForDashboardLoad(): Promise<void> {
    await expect(this.welcomeHeading).toBeVisible({ timeout: 30_000 });
    await expect(this.statsSection).toBeVisible({ timeout: 30_000 });
    await expect(this.activeDagsCard).toBeVisible({ timeout: 30_000 });
  }

  /**
   * Get the count from a stats card
   */
  // eslint-disable-next-line @typescript-eslint/class-methods-use-this
  private async getStatsCardCount(card: Locator): Promise<number> {
    await expect(card).toBeVisible();
    const badgeText = await card.locator("span").first().textContent();
    const match = badgeText?.match(/\d+/);

    if (!match) {
      throw new Error("Could not find count in stats card");
    }

    return parseInt(match[0], 10);
  }
}
