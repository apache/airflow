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
   * Apply a filter by selecting a filter type and value
   */
  public async applyFilter(filterName: string, filterValue: string): Promise<void> {
    const addFilterButton = this.page.locator(
      'button:has-text("Filter"), button:has-text("Add Filter"), button[aria-label*="filter" i]',
    );

    await expect(addFilterButton.first()).toBeVisible({ timeout: 5000 });
    await addFilterButton.first().click();

    const filterMenu = this.page.locator('[role="menu"], [role="menuitem"]');

    await expect(filterMenu.first()).toBeVisible({ timeout: 5000 });

    const filterOption = this.page.locator(`[role="menuitem"]:has-text("${filterName}")`).first();

    await expect(filterOption).toBeVisible({ timeout: 5000 });
    await filterOption.click();

    const filterPill = this.page.locator(`[role="combobox"], button:has-text("${filterName}:")`);

    await expect(filterPill.first()).toBeVisible({ timeout: 5000 });
    await filterPill.first().click();

    const dropdown = this.page.locator('[role="option"], [role="listbox"]');

    await expect(dropdown.first()).toBeVisible({ timeout: 5000 });

    const valueOption = this.page.locator(`[role="option"]:has-text("${filterValue}")`).first();

    await expect(valueOption).toBeVisible({ timeout: 5000 });
    await valueOption.click();

    await this.page.waitForLoadState("networkidle");
  }

  /**
   * Navigate to DAG Runs page
   */
  public async navigate(): Promise<void> {
    await this.navigateTo(DagRunsPage.dagRunsUrl);
    await this.page.waitForURL(/.*dag_runs/, { timeout: 15_000 });
    await this.dagRunsTable.waitFor({ state: "visible", timeout: 10_000 });

    const rows = this.dagRunsTable.locator('tbody tr:not(.no-data), div[role="row"]:not(:first-child)');
    const noDataMessage = this.page.locator('text="No Dag Runs found"');

    try {
      await expect(rows.first().or(noDataMessage)).toBeVisible({ timeout: 10_000 });
    } catch {
      await expect(this.dagRunsTable).toBeVisible();
    }
  }

  /**
   * Verify that DAG ID filtering works correctly
   */
  public async verifyDagIdFiltering(dagIdPattern: string): Promise<void> {
    const addFilterButton = this.page.locator(
      'button:has-text("Filter"), button:has-text("Add Filter"), button[aria-label*="filter" i]',
    );

    await expect(addFilterButton.first()).toBeVisible({ timeout: 5000 });
    await addFilterButton.first().click();

    const filterMenu = this.page.locator('[role="menu"], [role="menuitem"]');

    await expect(filterMenu.first()).toBeVisible({ timeout: 5000 });

    const dagIdFilterOption = this.page.locator('[role="menuitem"]:has-text("DAG ID")').first();

    await expect(dagIdFilterOption).toBeVisible({ timeout: 5000 });

    const inputsBeforeClick = await this.page.locator("input").count();

    await dagIdFilterOption.click();

    await expect(filterMenu.first()).not.toBeVisible({ timeout: 5000 });

    await this.page.waitForFunction(
      (expectedCount: number) => document.querySelectorAll("input").length > expectedCount,
      inputsBeforeClick,
      { timeout: 5000 },
    );

    const filterInput = this.page.locator("input").last();

    await expect(filterInput).toBeVisible({ timeout: 5000 });

    await filterInput.fill(dagIdPattern);
    await filterInput.press("Enter");

    await this.page.waitForURL(/.*dag_id_pattern=.*/, { timeout: 10_000 });
    await this.page.waitForLoadState("networkidle");
    await expect(this.dagRunsTable).toBeVisible();

    const rowsAfterFilter = this.dagRunsTable.locator(
      'tbody tr:not(.no-data), div[role="row"]:not(:first-child)',
    );
    const countAfter = await rowsAfterFilter.count();

    // Verify that we have results after filtering
    expect(countAfter).toBeGreaterThan(0);

    const rows = this.dagRunsTable.locator('tbody tr:not(.no-data), div[role="row"]:not(:first-child)');
    const rowsToCheck = Math.min(countAfter, 5);

    for (let i = 0; i < rowsToCheck; i++) {
      const row = rows.nth(i);
      // Get the first link in the row which should be the DAG ID
      const dagIdLink = row.locator("a[href*='/dags/']").first();
      const dagIdText = await dagIdLink.textContent();

      expect(dagIdText).toBeTruthy();
      expect(dagIdText).toContain(dagIdPattern);
    }
  }

  /**
   * Verify that DAG runs exist in the table
   */
  public async verifyDagRunsExist(): Promise<void> {
    const rows = this.dagRunsTable.locator('tbody tr:not(.no-data), div[role="row"]:not(:first-child)');

    expect(await rows.count()).toBeGreaterThan(0);
  }

  /**
   * Verify pagination works correctly with offset and limit parameters
   */
  public async verifyPagination(limit: number): Promise<void> {
    // Navigate with limit parameter
    await this.navigateTo(`${DagRunsPage.dagRunsUrl}?offset=0&limit=${limit}`);
    await this.page.waitForURL(/.*dag_runs/, { timeout: 10_000 });
    await this.dagRunsTable.waitFor({ state: "visible", timeout: 10_000 });

    const rows = this.dagRunsTable.locator('tbody tr:not(.no-data), div[role="row"]:not(:first-child)');
    const rowCount = await rows.count();

    // Verify we have results
    expect(rowCount).toBeGreaterThan(0);

    // Verify pagination controls exist
    const paginationInfo = this.page.locator("text=/\\d+\\s*-\\s*\\d+\\s*of\\s*\\d+/i");

    if ((await paginationInfo.count()) > 0) {
      await expect(paginationInfo.first()).toBeVisible();

      const paginationText = (await paginationInfo.first().textContent()) ?? "";

      expect(paginationText).toBeTruthy();

      // Extract the total count from pagination text (e.g., "1 - 10 of 50")
      const regex = /(\d+)\s*-\s*(\d+)\s*of\s*(\d+)/i;
      const match = regex.exec(paginationText);

      if (match?.[1] !== undefined && match[2] !== undefined && match[3] !== undefined) {
        const start = parseInt(match[1], 10);
        const end = parseInt(match[2], 10);
        const total = parseInt(match[3], 10);

        expect(start).toBeGreaterThan(0);
        expect(end).toBeGreaterThanOrEqual(start);
        expect(total).toBeGreaterThanOrEqual(end);

        // Test that offset changes when navigating to next page
        const initialStart = start;

        await this.navigateTo(`${DagRunsPage.dagRunsUrl}?offset=${limit}&limit=${limit}`);
        await this.page.waitForURL(/.*dag_runs/, { timeout: 10_000 });
        await this.dagRunsTable.waitFor({ state: "visible", timeout: 10_000 });

        const paginationInfoPage2 = this.page.locator("text=/\\d+\\s*-\\s*\\d+\\s*of\\s*\\d+/i");

        if ((await paginationInfoPage2.count()) > 0) {
          const paginationText2 = (await paginationInfoPage2.first().textContent()) ?? "";
          const regex2 = /(\d+)\s*-\s*(\d+)\s*of\s*(\d+)/i;
          const match2 = regex2.exec(paginationText2);

          if (match2?.[1] !== undefined) {
            const startPage2 = parseInt(match2[1], 10);

            // Verify that the start position changed
            expect(startPage2).toBeGreaterThan(initialStart);
          }
        }
      }
    }
  }

  /**
   * Verify that run details are displayed correctly
   */
  public async verifyRunDetailsDisplay(): Promise<void> {
    const firstRow = this.dagRunsTable.locator("tbody tr, div[role='row']:not(:first-child)").first();

    const dagIdLink = firstRow.locator("a[href*='/dags/']").first();

    if ((await dagIdLink.count()) > 0) {
      await expect(dagIdLink).toBeVisible();
      expect((await dagIdLink.textContent())?.trim()).toBeTruthy();
    }

    const runIdLink = firstRow.locator("a[href*='/runs/']").first();

    await expect(runIdLink).toBeVisible();
    expect((await runIdLink.textContent())?.trim()).toBeTruthy();

    const stateCell = firstRow
      .locator('td, div[role="cell"]')
      .filter({ hasText: /running|success|failed|queued/i });

    await expect(stateCell.first()).toBeVisible();

    const timeElements = firstRow.locator("time");

    if ((await timeElements.count()) > 0) {
      await expect(timeElements.first()).toBeVisible();
    } else {
      const cellTexts = await firstRow.locator("td, div[role='cell']").allTextContents();
      const hasDateFormat = cellTexts.some((text) =>
        /\d{4}(?:-\d{2}){2}|(?:\d{1,2}\/){2}\d{4}|(?:\d{1,2}:){2}\d{2}/.test(text),
      );

      expect(hasDateFormat).toBeTruthy();
    }
  }

  /**
   * Verify that state filtering works correctly
   */
  public async verifyStateFiltering(expectedState: string): Promise<void> {
    const rowsBeforeFilter = this.dagRunsTable.locator(
      'tbody tr:not(.no-data), div[role="row"]:not(:first-child)',
    );
    const countBefore = await rowsBeforeFilter.count();

    await this.applyFilter("State", expectedState);

    await this.page.waitForURL(/.*state=.*/, { timeout: 10_000 });
    await this.page.waitForLoadState("networkidle");
    await expect(this.dagRunsTable).toBeVisible();

    const rowsAfterFilter = this.dagRunsTable.locator(
      'tbody tr:not(.no-data), div[role="row"]:not(:first-child)',
    );
    const countAfter = await rowsAfterFilter.count();

    if (countAfter > 0) {
      expect(countAfter).toBeLessThanOrEqual(countBefore);

      const stateCells = this.dagRunsTable
        .locator('[class*="badge"], [class*="Badge"]')
        .filter({ hasText: new RegExp(expectedState, "i") });

      await expect(stateCells.first()).toBeVisible({ timeout: 5000 });

      const badgeCount = await stateCells.count();

      expect(badgeCount).toBeGreaterThan(0);
      expect(badgeCount).toBeLessThanOrEqual(countAfter);
    }
  }
}
