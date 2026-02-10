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

export class TaskInstancesPage extends BasePage {
  public static get taskInstancesUrl(): string {
    return "/task_instances";
  }

  public readonly paginationNextButton: Locator;

  public readonly paginationPrevButton: Locator;

  public readonly taskInstancesTable: Locator;

  public constructor(page: Page) {
    super(page);
    this.paginationNextButton = page.locator('[data-testid="next"]');
    this.paginationPrevButton = page.locator('[data-testid="prev"]');
    this.taskInstancesTable = page.locator('table, div[role="table"]');
  }

  /**
   * Click next page button
   */
  public async clickNextPage(): Promise<void> {
    const initialTaskInstanceIds = await this.getTaskInstanceIds();

    await this.paginationNextButton.click();

    await expect
      .poll(() => this.getTaskInstanceIds(), { timeout: 10_000 })
      .not.toEqual(initialTaskInstanceIds);

    await this.waitForTaskInstanceList();
  }

  /**
   * Click previous page button
   */
  public async clickPrevPage(): Promise<void> {
    const initialTaskInstanceIds = await this.getTaskInstanceIds();

    await this.paginationPrevButton.click();

    await expect
      .poll(() => this.getTaskInstanceIds(), { timeout: 10_000 })
      .not.toEqual(initialTaskInstanceIds);
    await this.waitForTaskInstanceList();
  }

  /**
   * Get all task instance identifiers from the current page
   */
  public async getTaskInstanceIds(): Promise<Array<string>> {
    await this.waitForTaskInstanceList();
    const taskLinks = this.taskInstancesTable.locator("a[href*='/dags/']");
    const texts = await taskLinks.allTextContents();

    return texts.map((text) => text.trim()).filter((text) => text !== "");
  }

  /**
   * Navigate to Task Instances page and wait for data to load
   */
  public async navigate(): Promise<void> {
    await this.navigateTo(TaskInstancesPage.taskInstancesUrl);
    await this.page.waitForURL(/.*task_instances/, { timeout: 15_000 });
    await this.taskInstancesTable.waitFor({ state: "visible", timeout: 10_000 });

    const dataLink = this.taskInstancesTable.locator("a[href*='/dags/']").first();
    const noDataMessage = this.page.locator('text="No Task Instances found"');

    await expect(dataLink.or(noDataMessage)).toBeVisible({ timeout: 30_000 });
  }

  /**
   * Verify state filtering via URL parameters
   */
  public async verifyStateFiltering(expectedState: string): Promise<void> {
    await this.navigateTo(`${TaskInstancesPage.taskInstancesUrl}?task_state=${expectedState.toLowerCase()}`);
    await this.page.waitForURL(/.*task_state=.*/, { timeout: 15_000 });
    await this.page.waitForLoadState("networkidle");

    const dataLink = this.taskInstancesTable.locator("a[href*='/dags/']").first();

    await expect(dataLink).toBeVisible({ timeout: 30_000 });
    await expect(this.taskInstancesTable).toBeVisible();

    const rowsAfterFilter = this.taskInstancesTable.locator(
      'tbody tr:not(.no-data), div[role="row"]:not(:first-child)',
    );
    const noDataMessage = this.page.locator("text=/No.*found/i, text=/No.*results/i, text=/Empty/i");
    const stateBadges = this.taskInstancesTable.locator('[class*="badge"], [class*="Badge"]');

    await expect(stateBadges.first().or(noDataMessage.first())).toBeVisible({ timeout: 30_000 });

    const countAfter = await rowsAfterFilter.count();

    expect(
      countAfter,
      `Expected task instances with state "${expectedState}" but found none`,
    ).toBeGreaterThan(0);

    const badgeCount = await stateBadges.count();

    expect(badgeCount).toBeGreaterThan(0);

    for (let i = 0; i < Math.min(badgeCount, 20); i++) {
      const badge = stateBadges.nth(i);
      const badgeText = (await badge.textContent())?.trim().toLowerCase();

      expect(badgeText).toContain(expectedState.toLowerCase());
    }
  }

  /**
   * Verify that task instance details are displayed correctly
   */
  public async verifyTaskDetailsDisplay(): Promise<void> {
    const firstRow = this.taskInstancesTable.locator("tbody tr, div[role='row']:not(:first-child)").first();

    const dagIdLink = firstRow.locator("a[href*='/dags/']").first();

    if ((await dagIdLink.count()) > 0) {
      await expect(dagIdLink).toBeVisible();
      expect((await dagIdLink.textContent())?.trim()).toBeTruthy();
    }

    const allCells = firstRow.locator('td, div[role="cell"]');
    const cellCount = await allCells.count();

    expect(cellCount).toBeGreaterThan(1);

    const runIdLink = firstRow.locator("a[href*='/runs/']").first();

    if ((await runIdLink.count()) > 0) {
      await expect(runIdLink).toBeVisible();
      expect((await runIdLink.textContent())?.trim()).toBeTruthy();
    }

    const stateBadge = firstRow.locator('[class*="badge"], [class*="Badge"], [class*="status"]');

    const hasStateBadge = (await stateBadge.count()) > 0;

    if (hasStateBadge) {
      await expect(stateBadge.first()).toBeVisible();
    } else {
      const allCellsForState = firstRow.locator('td, div[role="cell"]');

      expect(await allCellsForState.count()).toBeGreaterThan(2);
    }

    const timeElements = firstRow.locator("time");

    if ((await timeElements.count()) > 0) {
      await expect(timeElements.first()).toBeVisible();
    } else {
      const dateCells = firstRow.locator('td, div[role="cell"]');
      const dateCellCount = await dateCells.count();

      expect(dateCellCount).toBeGreaterThan(3);
    }
  }

  /**
   * Verify that task instances exist in the table
   */
  public async verifyTaskInstancesExist(): Promise<void> {
    const rows = this.taskInstancesTable.locator('tbody tr:not(.no-data), div[role="row"]:not(:first-child)');

    expect(await rows.count()).toBeGreaterThan(0);
  }

  /**
   * Wait for task instance list to be rendered
   */
  private async waitForTaskInstanceList(): Promise<void> {
    const dataLink = this.taskInstancesTable.locator("a[href*='/dags/']").first();

    await expect(dataLink).toBeVisible({ timeout: 10_000 });
  }
}
