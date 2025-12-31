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

    await this.backfillFromDateInput.fill(fromDate);
    await this.backfillToDateInput.fill(toDate);

    await this.selectReprocessBehavior(reprocessBehavior);

    const runsMessage = this.page.locator("text=/\\d+ runs? will be triggered|No runs matching/");

    await expect(runsMessage).toBeVisible({ timeout: 20_000 });

    const hasRuns = await this.page.locator("text=/\\d+ runs? will be triggered/").isVisible();

    if (!hasRuns) {
      await this.page.keyboard.press("Escape");

      return;
    }

    await expect(this.backfillRunButton).toBeVisible({ timeout: 20_000 });
    await this.backfillRunButton.scrollIntoViewIfNeeded();
    await this.backfillRunButton.click({ timeout: 20_000 });
  }

  public async findBackfillRowByDateRange(identifier: BackfillRowIdentifier): Promise<Locator> {
    const { fromDate: expectedFrom, toDate: expectedTo } = identifier;

    await this.backfillsTable.waitFor({ state: "visible", timeout: 10_000 });

    const columnMap = await this.getColumnIndexMap();
    const fromIndex = columnMap.get("From");
    const toIndex = columnMap.get("To");

    if (fromIndex === undefined || toIndex === undefined) {
      const availableColumns = [...columnMap.keys()].join(", ");

      throw new Error(
        `Required columns "From" and/or "To" not found. Available columns: [${availableColumns}]`,
      );
    }

    const rows = this.page.locator("table tbody tr");
    const rowCount = await rows.count();

    if (rowCount === 0) {
      throw new Error(
        `No backfill rows found in table. Expected to find backfill with From: "${expectedFrom}", To: "${expectedTo}"`,
      );
    }

    for (let i = 0; i < rowCount; i++) {
      const row = rows.nth(i);
      const cells = row.locator("td");
      const fromCell = (await cells.nth(fromIndex).textContent()) ?? "";
      const toCell = (await cells.nth(toIndex).textContent()) ?? "";

      if (datesMatch(fromCell, expectedFrom) && datesMatch(toCell, expectedTo)) {
        return row;
      }
    }

    const availableRows: Array<string> = [];

    for (let i = 0; i < rowCount; i++) {
      const row = rows.nth(i);
      const cells = row.locator("td");
      const fromCell = (await cells.nth(fromIndex).textContent()) ?? "";
      const toCell = (await cells.nth(toIndex).textContent()) ?? "";

      availableRows.push(`  Row ${i}: From="${fromCell.trim()}", To="${toCell.trim()}"`);
    }

    throw new Error(
      `Backfill not found with From: "${expectedFrom}" (normalized: ${normalizeDate(expectedFrom)}), ` +
        `To: "${expectedTo}" (normalized: ${normalizeDate(expectedTo)})\n` +
        `Available rows:\n${availableRows.join("\n")}`,
    );
  }

  public async getBackfillDetailsByDateRange(identifier: BackfillRowIdentifier): Promise<BackfillDetails> {
    const row = await this.findBackfillRowByDateRange(identifier);
    const cells = row.locator("td");
    const columnMap = await this.getColumnIndexMap();

    const fromIndex = columnMap.get("From") ?? 0;
    const toIndex = columnMap.get("To") ?? 1;
    const reprocessIndex = columnMap.get("Reprocess Behavior") ?? 2;
    const createdAtIndex = columnMap.get("Created at") ?? 3;

    const [fromDate, toDate, reprocessBehavior, createdAt] = await Promise.all([
      cells.nth(fromIndex).textContent(),
      cells.nth(toIndex).textContent(),
      cells.nth(reprocessIndex).textContent(),
      cells.nth(createdAtIndex).textContent(),
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

  public async getBackfillStatus(): Promise<string> {
    const statusIcon = this.page.getByTestId("state-badge").first();

    await expect(statusIcon).toBeVisible();
    await statusIcon.click({ timeout: 20_000 });

    const statusBadge = this.page.getByTestId("state-badge").first();

    await expect(statusBadge).toBeVisible();

    const statusText = (await statusBadge.textContent()) ?? "";

    return statusText.trim();
  }

  public getColumnHeader(columnName: string): Locator {
    return this.page.locator(`th:has-text("${columnName}")`);
  }

  public async getColumnIndex(columnName: string): Promise<number> {
    const columnMap = await this.getColumnIndexMap();
    const index = columnMap.get(columnName);

    if (index === undefined) {
      const availableColumns = [...columnMap.keys()].join(", ");

      throw new Error(`Column "${columnName}" not found in table. Available columns: [${availableColumns}]`);
    }

    return index;
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

    await expect(backfillInProgress).not.toBeVisible({ timeout: 120_000 });
  }
}
