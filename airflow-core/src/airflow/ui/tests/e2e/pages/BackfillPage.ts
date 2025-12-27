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
import { expect } from "@playwright/test";
import type { Locator, Page } from "@playwright/test";
import { BasePage } from "tests/e2e/pages/BasePage";

export type ReprocessBehavior = "All Runs" | "Missing and Errored Runs" | "Missing Runs";

export type CreateBackfillOptions = {
  fromDate: string;
  reprocessBehavior: ReprocessBehavior;
  toDate: string;
};

export type VerifyBackfillOptions = {
  dagName: string;
  expectedFromDate: string;
  expectedToDate: string;
  reprocessBehavior: ReprocessBehavior;
};

export class BackfillPage extends BasePage {
  public readonly backfillDateError: Locator;
  public readonly backfillFromDateInput: Locator;
  public readonly backfillModeRadio: Locator;
  public readonly backfillRunButton: Locator;
  public readonly backfillsTable: Locator;
  public readonly backfillToDateInput: Locator;
  public readonly triggerButton: Locator;

  public constructor(page: Page) {
    super(page);
    this.triggerButton = page.locator('button[aria-label="Trigger Dag"]:has-text("Trigger")');
    this.backfillModeRadio = page.locator('label:has-text("Backfill")');
    this.backfillFromDateInput = page.locator('input[type="datetime-local"]').first();
    this.backfillToDateInput = page.locator('input[type="datetime-local"]').nth(1);
    this.backfillRunButton = page.locator('button:has-text("Run Backfill")');
    this.backfillsTable = page.locator("table");
    this.backfillDateError = page.locator('text="Start Date must be before the End Date"');
  }

  public static getBackfillsUrl(dagName: string): string {
    return `/dags/${dagName}/backfills`;
  }

  public static getDagDetailUrl(dagName: string): string {
    return `/dags/${dagName}`;
  }

  public async createBackfill(dagName: string, options: CreateBackfillOptions): Promise<void> {
    const { fromDate, reprocessBehavior, toDate } = options;

    await this.navigateToDagDetail(dagName);
    await this.openBackfillDialog();

    await this.backfillFromDateInput.fill(fromDate);
    await this.backfillToDateInput.fill(toDate);

    await this.selectReprocessBehavior(reprocessBehavior);

    const runsMessage = this.page.locator("text=/\\d+ runs? will be triggered|No runs matching/");

    await expect(runsMessage).toBeVisible({ timeout: 10_000 });

    await expect(this.backfillRunButton).toBeEnabled({ timeout: 5000 });
    await this.backfillRunButton.click();
  }

  // Get backfill details from a specific row (default: first row)
  public async getBackfillDetails(rowIndex: number = 0): Promise<{
    createdAt: string;
    fromDate: string;
    reprocessBehavior: string;
    toDate: string;
  }> {
    const row = this.page.locator("table tbody tr").nth(rowIndex);
    const cells = row.locator("td");

    const fromDate = (await cells.nth(0).textContent()) ?? "";
    const toDate = (await cells.nth(1).textContent()) ?? "";
    const reprocessBehavior = (await cells.nth(2).textContent()) ?? "";
    const createdAt = (await cells.nth(3).textContent()) ?? "";

    return {
      createdAt: createdAt.trim(),
      fromDate: fromDate.trim(),
      reprocessBehavior: reprocessBehavior.trim(),
      toDate: toDate.trim(),
    };
  }

  public async getBackfillsTableRows(): Promise<number> {
    const rows = this.page.locator("table tbody tr");

    await rows.first().waitFor({ state: "visible", timeout: 10_000 });
    const count = await rows.count();

    return count;
  }

  // Get filter button
  public getFilterButton(): Locator {
    return this.page.locator('button[aria-label*="filter"], button[aria-label*="Filter"]');
  }

  // Get number of table columns
  public async getTableColumnCount(): Promise<number> {
    const headers = this.page.locator("table thead th");

    return await headers.count();
  }

  public async isBackfillDateErrorVisible(): Promise<boolean> {
    return this.backfillDateError.isVisible();
  }

  //  Check if a specific column is visible in the table
  public async isColumnVisible(columnName: string): Promise<boolean> {
    const header = this.page.locator(`th:has-text("${columnName}")`);

    try {
      await header.waitFor({ state: "visible", timeout: 2000 });

      return true;
    } catch {
      return false;
    }
  }

  // Check if filter button is available
  public async isFilterAvailable(): Promise<boolean> {
    const filterButton = this.getFilterButton();

    try {
      await filterButton.waitFor({ state: "visible", timeout: 2000 });

      return true;
    } catch {
      return false;
    }
  }

  public async navigateToBackfillsTab(dagName: string): Promise<void> {
    await this.navigateTo(BackfillPage.getBackfillsUrl(dagName));
  }

  public async navigateToDagDetail(dagName: string): Promise<void> {
    await this.navigateTo(BackfillPage.getDagDetailUrl(dagName));
  }

  public async openBackfillDialog(): Promise<void> {
    await this.triggerButton.waitFor({ state: "visible", timeout: 10_000 });
    await this.triggerButton.click();

    await expect(this.backfillModeRadio).toBeVisible({ timeout: 8000 });
    await this.backfillModeRadio.click();

    await expect(this.backfillFromDateInput).toBeVisible({ timeout: 5000 });
  }

  // Open the filter menu
  public async openFilterMenu(): Promise<void> {
    const filterButton = this.getFilterButton();

    await filterButton.click();

    // Wait for menu to appear
    const filterMenu = this.page.locator('[role="menu"]');

    await filterMenu.waitFor({ state: "visible", timeout: 5000 });
  }

  public async selectReprocessBehavior(behavior: ReprocessBehavior): Promise<void> {
    const behaviorLabels: Record<ReprocessBehavior, string> = {
      "All Runs": "All Runs",
      "Missing and Errored Runs": "Missing and Errored Runs",
      "Missing Runs": "Missing Runs",
    };

    const label = behaviorLabels[behavior];
    const radioItem = this.page.locator(`label:has-text("${label}")`).first();

    await radioItem.waitFor({ state: "visible", timeout: 5000 });
    await radioItem.click();
  }

  //  Toggle a column's visibility in the filter menu
  public async toggleColumn(columnName: string): Promise<void> {
    const menuItem = this.page.locator(`[role="menuitem"]:has-text("${columnName}")`);

    await menuItem.click();
  }
}
