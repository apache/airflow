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

export type BackfillDetails = {
  createdAt: string;
  fromDate: string;
  reprocessBehavior: string;
  toDate: string;
};

export type BackfillRowIdentifier = {
  fromDate: string;
  toDate: string;
};

function normalizeDate(dateString: string): string {
  const trimmed = dateString.trim();

  if (trimmed.includes("T")) {
    const parts = trimmed.split("T");

    return parts[0] ?? trimmed;
  }

  if (trimmed.includes(" ")) {
    const parts = trimmed.split(" ");

    return parts[0] ?? trimmed;
  }

  return trimmed;
}

function datesMatch(date1: string, date2: string): boolean {
  return normalizeDate(date1) === normalizeDate(date2);
}

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

  public static findColumnIndex(columnMap: Map<string, number>, possibleNames: Array<string>): number {
    for (const name of possibleNames) {
      const index = columnMap.get(name);

      if (index !== undefined) {
        return index;
      }
    }

    return -1;
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
    await this.waitForNoActiveBackfill();
    await this.openBackfillDialog();

    await this.backfillFromDateInput.click();
    await this.backfillFromDateInput.fill(fromDate);
    await this.backfillFromDateInput.dispatchEvent("change");

    await this.backfillToDateInput.click();
    await this.backfillToDateInput.fill(toDate);
    await this.backfillToDateInput.dispatchEvent("change");

    await this.page.waitForTimeout(500);

    await this.selectReprocessBehavior(reprocessBehavior);

    const runsWillBeTriggered = this.page.locator("text=/\\d+ runs? will be triggered/");
    const noRunsMatching = this.page.locator("text=/No runs matching/");

    await expect(runsWillBeTriggered.or(noRunsMatching)).toBeVisible({ timeout: 20_000 });

    let previousText = "";
    let stableCount = 0;

    while (stableCount < 3) {
      await this.page.waitForTimeout(500);
      const currentText = (await runsWillBeTriggered.or(noRunsMatching).textContent()) ?? "";

      if (currentText === previousText) {
        stableCount++;
      } else {
        stableCount = 0;
        previousText = currentText;
      }
    }

    const hasRuns = await runsWillBeTriggered.isVisible();

    if (!hasRuns) {
      await this.page.keyboard.press("Escape");

      return;
    }

    await expect(this.backfillRunButton).toBeVisible({ timeout: 20_000 });
    await this.backfillRunButton.scrollIntoViewIfNeeded();
    await this.backfillRunButton.click({ timeout: 20_000 });
    await this.page.waitForLoadState("networkidle", { timeout: 30_000 });
  }

  public async findBackfillRowByDateRange(
    identifier: BackfillRowIdentifier,
    timeout: number = 180_000,
  ): Promise<Locator> {
    const { fromDate: expectedFrom, toDate: expectedTo } = identifier;

    await this.backfillsTable.waitFor({ state: "visible", timeout: 10_000 });
    await this.waitForTableDataLoaded();

    const columnMap = await this.getColumnIndexMap();
    const fromIndex = BackfillPage.findColumnIndex(columnMap, ["From", "table.from"]);
    const toIndex = BackfillPage.findColumnIndex(columnMap, ["To", "table.to"]);

    if (fromIndex === -1 || toIndex === -1) {
      const availableColumns = [...columnMap.keys()].join(", ");

      throw new Error(
        `Required columns "From" and/or "To" not found. Available columns: [${availableColumns}]`,
      );
    }

    let foundRow: Locator | undefined;

    await expect(async () => {
      await this.page.reload();
      await this.backfillsTable.waitFor({ state: "visible", timeout: 10_000 });
      await this.waitForTableDataLoaded();

      const rows = this.page.locator("table tbody tr");
      const rowCount = await rows.count();

      for (let i = 0; i < rowCount; i++) {
        const row = rows.nth(i);
        const cells = row.locator("td");
        const fromCell = (await cells.nth(fromIndex).textContent()) ?? "";
        const toCell = (await cells.nth(toIndex).textContent()) ?? "";

        if (datesMatch(fromCell, expectedFrom) && datesMatch(toCell, expectedTo)) {
          foundRow = row;

          return;
        }
      }

      throw new Error("Backfill not yet visible");
    }).toPass({ timeout });

    // toPass() guarantees foundRow is set when it succeeds
    return foundRow as Locator;
  }

  public async getBackfillDetailsByDateRange(identifier: BackfillRowIdentifier): Promise<BackfillDetails> {
    const row = await this.findBackfillRowByDateRange(identifier);
    const cells = row.locator("td");
    const columnMap = await this.getColumnIndexMap();

    const fromIndex = BackfillPage.findColumnIndex(columnMap, ["From", "table.from"]);
    const toIndex = BackfillPage.findColumnIndex(columnMap, ["To", "table.to"]);
    const reprocessIndex = BackfillPage.findColumnIndex(columnMap, [
      "Reprocess Behavior",
      "backfill.reprocessBehavior",
    ]);
    const createdAtIndex = BackfillPage.findColumnIndex(columnMap, ["Created at", "table.createdAt"]);

    const [fromDate, toDate, reprocessBehavior, createdAt] = await Promise.all([
      cells.nth(fromIndex === -1 ? 0 : fromIndex).textContent(),
      cells.nth(toIndex === -1 ? 1 : toIndex).textContent(),
      cells.nth(reprocessIndex === -1 ? 2 : reprocessIndex).textContent(),
      cells.nth(createdAtIndex === -1 ? 3 : createdAtIndex).textContent(),
    ]);

    return {
      createdAt: (createdAt ?? "").trim(),
      fromDate: (fromDate ?? "").trim(),
      reprocessBehavior: (reprocessBehavior ?? "").trim(),
      toDate: (toDate ?? "").trim(),
    };
  }

  public async getBackfillsTableRows(): Promise<number> {
    const rows = this.page.locator("table tbody tr");

    try {
      await rows.first().waitFor({ state: "visible", timeout: 5000 });
    } catch {
      return 0;
    }

    return await rows.count();
  }

  public async getBackfillStatus(rowIndex: number = 0): Promise<string> {
    const row = this.page.locator("table tbody tr").nth(rowIndex);

    await expect(row).toBeVisible({ timeout: 10_000 });

    const headers = this.page.locator("table thead th");
    const headerTexts = await headers.allTextContents();
    const completedAtIndex = headerTexts.findIndex((text) => text.toLowerCase().includes("completed"));

    if (completedAtIndex !== -1) {
      const completedCell = row.locator("td").nth(completedAtIndex);
      const completedText = ((await completedCell.textContent()) ?? "").trim();

      return completedText ? "Completed" : "Running";
    }

    return "Running";
  }

  public getColumnHeader(columnName: string): Locator {
    return this.page.locator(`th:has-text("${columnName}")`);
  }

  public async getColumnIndexMap(): Promise<Map<string, number>> {
    const headers = this.page.locator("table thead th");

    await headers.first().waitFor({ state: "visible", timeout: 10_000 });
    const headerTexts = await headers.allTextContents();

    return new Map(headerTexts.map((text, index) => [text.trim(), index]));
  }

  public getFilterButton(): Locator {
    return this.page.locator(
      'button[aria-label*="Filter table columns"], button:has-text("Filter table columns")',
    );
  }

  public async getTableColumnCount(): Promise<number> {
    const headers = this.page.locator("table thead th");

    return await headers.count();
  }

  public async navigateToBackfillsTab(dagName: string): Promise<void> {
    await this.navigateTo(BackfillPage.getBackfillsUrl(dagName));
    await expect(this.backfillsTable).toBeVisible({ timeout: 20_000 });
  }

  public async navigateToDagDetail(dagName: string): Promise<void> {
    await this.navigateTo(BackfillPage.getDagDetailUrl(dagName));
    await this.page.waitForLoadState("networkidle");
  }

  public async openBackfillDialog(): Promise<void> {
    await expect(this.triggerButton).toBeVisible({ timeout: 20_000 });
    await this.triggerButton.click();

    await expect(this.backfillModeRadio).toBeVisible({ timeout: 20_000 });
    await this.backfillModeRadio.click();

    await expect(this.backfillFromDateInput).toBeVisible({ timeout: 20_000 });
  }

  public async openFilterMenu(): Promise<void> {
    const filterButton = this.getFilterButton();

    await filterButton.click();

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

  public async toggleColumn(columnName: string): Promise<void> {
    const menuItem = this.page.locator(`[role="menuitem"]:has-text("${columnName}")`);

    await menuItem.click();
  }

  public async waitForNoActiveBackfill(): Promise<void> {
    const backfillInProgress = this.page.locator('text="Backfill in progress:"');

    await expect(backfillInProgress).not.toBeVisible({ timeout: 300_000 });
  }

  public async waitForTableDataLoaded(): Promise<void> {
    const firstCell = this.page.locator("table tbody tr:first-child td:first-child");

    await expect(firstCell).toBeVisible({ timeout: 30_000 });
    await expect(async () => {
      const text = await firstCell.textContent();

      if (text === null || text.trim() === "") {
        throw new Error("Table data still loading");
      }
    }).toPass({ timeout: 30_000 });
  }
}
