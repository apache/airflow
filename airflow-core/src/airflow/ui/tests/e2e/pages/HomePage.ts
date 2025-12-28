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
import type { Locator, Page } from "@playwright/test";
import { BasePage } from "tests/e2e/pages/BasePage";

/**
 * Home/Dashboard Page Object
 */
export class HomePage extends BasePage {
  // Page URLs
  public static get homeUrl(): string {
    return "/";
  }

  public readonly activeDagsCard: Locator;
  public readonly dagImportErrorsCard: Locator;
  public readonly dagProcessorHealth: Locator;
  public readonly dagRunMetrics: Locator;
  public readonly failedDagsCard: Locator;

  // Health section elements
  public readonly healthSection: Locator;
  // Historical Metrics section (recent runs)
  public readonly historicalMetricsSection: Locator;
  public readonly metaDatabaseHealth: Locator;
  // Pool Summary section
  public readonly poolSummarySection: Locator;
  public readonly runningDagsCard: Locator;

  public readonly schedulerHealth: Locator;

  // Dashboard Stats elements
  public readonly statsSection: Locator;
  public readonly taskInstanceMetrics: Locator;
  public readonly triggererHealth: Locator;

  public constructor(page: Page) {
    super(page);

    // Stats cards - using link patterns that match the StatsCard component
    this.failedDagsCard = page.locator('a[href*="last_dag_run_state=failed"]');
    this.runningDagsCard = page.locator('a[href*="last_dag_run_state=running"]');
    this.activeDagsCard = page.locator('a[href*="paused=false"]');
    this.dagImportErrorsCard = page.locator('button:has-text("DAG Import Errors")');

    // Stats section - using role-based selector
    this.statsSection = page.getByRole("heading", { name: "Stats" }).locator("..");
    // Health section - using role-based selector
    this.healthSection = page.getByRole("heading", { name: "Health" }).locator("..");
    this.metaDatabaseHealth = page.getByText("Metadatabase").first();
    this.schedulerHealth = page.getByText("Scheduler").first();
    this.triggererHealth = page.getByText("Triggerer").first();
    this.dagProcessorHealth = page.getByText("DAG Processor").first();

    // Pool Summary section - using role-based selector
    this.poolSummarySection = page.getByRole("heading", { name: "Pool Summary" }).locator("..");

    // Historical Metrics section (recent runs) - using role-based selector
    this.historicalMetricsSection = page.getByRole("heading", { name: "History" }).locator("..");
    this.dagRunMetrics = page.getByRole("heading", { name: /dag run/i }).first();
    this.taskInstanceMetrics = page.getByRole("heading", { name: /task instance/i }).first();
  }

  /**
   * Check if health badges are visible
   */
  public async areHealthBadgesVisible(): Promise<boolean> {
    try {
      await this.metaDatabaseHealth.waitFor({ state: "visible", timeout: 10_000 });
      await this.schedulerHealth.waitFor({ state: "visible", timeout: 5000 });

      return true;
    } catch {
      return false;
    }
  }

  /**
   * Get Active DAGs count
   */
  public async getActiveDagsCount(): Promise<number> {
    return this.getStatsCardCount(this.activeDagsCard);
  }

  /**
   * Get Failed DAGs count
   */
  public async getFailedDagsCount(): Promise<number> {
    return this.getStatsCardCount(this.failedDagsCard);
  }

  /**
   * Get Running DAGs count
   */
  public async getRunningDagsCount(): Promise<number> {
    return this.getStatsCardCount(this.runningDagsCard);
  }

  /**
   * Check if DAG Import Errors are displayed (only visible when errors exist)
   */
  public async isDagImportErrorsVisible(): Promise<boolean> {
    try {
      return await this.dagImportErrorsCard.isVisible();
    } catch {
      return false;
    }
  }

  /**
   * Check if historical metrics (recent runs) section is visible
   */
  public async isHistoricalMetricsSectionVisible(): Promise<boolean> {
    try {
      await this.dagRunMetrics.waitFor({ state: "visible", timeout: 10_000 });

      return true;
    } catch {
      return false;
    }
  }

  /**
   * Check if stats section is visible
   */
  public async isStatsSectionVisible(): Promise<boolean> {
    try {
      await this.activeDagsCard.waitFor({ state: "visible", timeout: 10_000 });

      return true;
    } catch {
      return false;
    }
  }

  /**
   * Navigate to Home/Dashboard page
   */
  public async navigate(): Promise<void> {
    await this.navigateTo(HomePage.homeUrl);
  }

  /**
   * Wait for dashboard to fully load
   */
  public async waitForDashboardLoad(): Promise<void> {
    await this.welcomeHeading.waitFor({ state: "visible", timeout: 30_000 });
  }

  /**
   * Get the count from a stats card
   */
  // eslint-disable-next-line @typescript-eslint/class-methods-use-this
  private async getStatsCardCount(card: Locator): Promise<number> {
    await card.waitFor({ state: "visible" }); // Fail fast if card doesn't exist
    const badgeText = await card.locator("span").first().textContent();
    const match = badgeText?.match(/\d+/);

    if (!match) {
      throw new Error("Could not find count in stats card");
    }

    return parseInt(match[0], 10);
  }
}
