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
 * Page Object for the DAG Runs page (/dag_runs)
 */
export class DagRunsPage extends BasePage {
  public static get dagRunsUrl(): string {
    return "/dag_runs";
  }

  public readonly dagRunsTable: Locator;

  public constructor(page: Page) {
    super(page);
    this.dagRunsTable = page.locator('table, div[role="table"]');
  }

  /**
   * Navigate to DAG Runs page
   */
  public async navigate(): Promise<void> {
    await this.navigateTo(DagRunsPage.dagRunsUrl);
    await this.page.waitForURL(/.*dag_runs/, { timeout: 15_000 });
    await this.dagRunsTable.waitFor({ state: "visible", timeout: 10_000 });

    // Wait for data to load - check if we have rows or "No Dag Runs found" message
    const rows = this.dagRunsTable.locator('tbody tr:not(.no-data), div[role="row"]:not(:first-child)');
    const noDataMessage = this.page.locator('text="No Dag Runs found"');

    await Promise.race([
      rows
        .first()
        .waitFor({ state: "visible", timeout: 10_000 })
        .catch(() => {
          // Ignore error - either rows or no data message will appear
        }),
      noDataMessage.waitFor({ state: "visible", timeout: 10_000 }).catch(() => {
        // Ignore error - either rows or no data message will appear
      }),
    ]);
  }

  /**
   * Verify that DAG runs exist in the table
   */
  public async verifyDagRunsExist(): Promise<void> {
    const rows = this.dagRunsTable.locator('tbody tr:not(.no-data), div[role="row"]:not(:first-child)');

    expect(await rows.count()).toBeGreaterThan(0);
  }

  /**
   * Verify that filtering works
   */
  public async verifyFilteringWorks(): Promise<void> {
    const addFilterButton = this.page.locator(
      'button:has-text("Filter"), button:has-text("Add Filter"), button[aria-label*="filter" i]',
    );

    await expect(addFilterButton.first()).toBeVisible({ timeout: 5000 });
    await addFilterButton.first().click();

    await this.page.waitForTimeout(1000);

    const stateFilterOption = this.page.locator(':text("State")').first();

    await expect(stateFilterOption).toBeVisible({ timeout: 5000 });

    await stateFilterOption.click();
    await this.page.waitForTimeout(500);

    await expect(this.dagRunsTable).toBeVisible();
  }

  /**
   * Verify that run details are displayed correctly
   * Checks: DAG ID, Run ID, State, Start Date, End Date
   */
  public async verifyRunDetailsDisplay(): Promise<void> {
    const firstRow = this.dagRunsTable.locator("tbody tr, div[role='row']:not(:first-child)").first();

    const dagIdLink = firstRow.locator("a[href*='/dags/']").first();

    if ((await dagIdLink.count()) > 0) {
      await expect(dagIdLink).toBeVisible();
      expect((await dagIdLink.textContent())?.trim()).toBeTruthy();
    }

    // Verify Run ID
    const runIdLink = firstRow.locator("a[href*='/runs/']").first();

    await expect(runIdLink).toBeVisible();
    expect((await runIdLink.textContent())?.trim()).toBeTruthy();

    // Verify State - look for text content containing state keywords
    const stateCell = firstRow
      .locator('td, div[role="cell"]')
      .filter({ hasText: /running|success|failed|queued/i });

    await expect(stateCell.first()).toBeVisible();

    // Verify dates (start/end time) - look for time elements or date patterns
    const timeElements = firstRow.locator("time");

    if ((await timeElements.count()) > 0) {
      await expect(timeElements.first()).toBeVisible();
    } else {
      // Fallback to text content search
      const cellTexts = await firstRow.locator("td, div[role='cell']").allTextContents();
      const hasDateFormat = cellTexts.some((text) =>
        /\d{4}(?:-\d{2}){2}|(?:\d{1,2}\/){2}\d{4}|(?:\d{1,2}:){2}\d{2}/.test(text),
      );

      expect(hasDateFormat).toBeTruthy();
    }
  }

  /**
   * Verify that different run states are visually distinct
   */
  public async verifyStateVisualDistinction(): Promise<void> {
    const stateBadges = this.dagRunsTable.locator('[class*="badge"], [class*="Badge"]');

    await stateBadges.first().waitFor({ state: "visible", timeout: 5000 });

    const badgeCount = await stateBadges.count();

    expect(badgeCount).toBeGreaterThan(0);

    const stateStyles = new Map<string, string>();
    const limit = Math.min(badgeCount, 10);

    for (let i = 0; i < limit; i++) {
      const badge = stateBadges.nth(i);
      const text = (await badge.textContent())?.trim().toLowerCase();
      const bgColor = await badge.evaluate((el) => window.getComputedStyle(el).backgroundColor);

      if (text !== "" && text !== undefined) {
        stateStyles.set(text, bgColor);
      }
    }

    // Verify at least one state badge exists
    expect(stateStyles.size).toBeGreaterThanOrEqual(1);

    // If multiple states exist, verify they have different colors
    if (stateStyles.size > 1) {
      const colors = [...stateStyles.values()];
      const uniqueColors = new Set(colors);

      expect(uniqueColors.size).toBeGreaterThan(1);
    }
  }
}
