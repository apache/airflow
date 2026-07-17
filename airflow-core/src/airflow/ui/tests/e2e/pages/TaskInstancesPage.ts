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

  public readonly taskInstancesTable: Locator;

  public constructor(page: Page) {
    super(page);
    this.taskInstancesTable = page.getByRole("table");
  }

  /**
   * Navigate to Task Instances page and wait for data to load
   */
  public async navigate(): Promise<void> {
    await expect(async () => {
      await this.navigateTo(TaskInstancesPage.taskInstancesUrl);
      await this.page.waitForURL(/.*task_instances/, { timeout: 15_000 });
      await expect(this.taskInstancesTable).toBeVisible();
    }).toPass({ intervals: [2000], timeout: 60_000 });

    const dataLink = this.taskInstancesTable.getByRole("link").first();
    const noDataMessage = this.page.getByText("No Task Instances found");

    await expect(dataLink.or(noDataMessage)).toBeVisible({ timeout: 30_000 });
  }

  /**
   * Verify state filtering via URL parameters
   */
  public async verifyStateFiltering(expectedState: string, dagId?: string): Promise<void> {
    const params = new URLSearchParams({ task_state: expectedState.toLowerCase() });

    if (dagId !== undefined) {
      params.set("dag_id", dagId);
    }
    await expect(async () => {
      await this.navigateTo(`${TaskInstancesPage.taskInstancesUrl}?${params.toString()}`);
      await this.page.waitForURL(/.*task_state=.*/, { timeout: 15_000 });
    }).toPass({ intervals: [2000], timeout: 60_000 });

    const dataLink = this.taskInstancesTable.getByRole("link").first();

    await expect(dataLink).toBeVisible({ timeout: 30_000 });
    await expect(this.taskInstancesTable).toBeVisible();

    // getByRole("row") returns all rows including the header; filter to data rows only.
    const rowsAfterFilter = this.taskInstancesTable
      .getByRole("row")
      .filter({ hasNot: this.taskInstancesTable.getByRole("columnheader") });
    const noDataMessage = this.page.getByText(/no .* found|no .* results/i);
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
    const firstRow = this.taskInstancesTable.getByRole("row").nth(1);

    const dagIdLink = firstRow.getByRole("link").first();

    if ((await dagIdLink.count()) > 0) {
      await expect(dagIdLink).toBeVisible();
      expect((await dagIdLink.textContent())?.trim()).toBeTruthy();
    }

    const allCells = firstRow.getByRole("cell");
    const cellCount = await allCells.count();

    expect(cellCount).toBeGreaterThan(1);

    const runIdLink = firstRow.getByRole("link").nth(1);

    if ((await runIdLink.count()) > 0) {
      await expect(runIdLink).toBeVisible();
      expect((await runIdLink.textContent())?.trim()).toBeTruthy();
    }

    const stateBadge = firstRow.locator('[class*="badge"], [class*="Badge"], [class*="status"]');

    const hasStateBadge = (await stateBadge.count()) > 0;

    if (hasStateBadge) {
      await expect(stateBadge.first()).toBeVisible();
    } else {
      const allCellsForState = firstRow.getByRole("cell");

      expect(await allCellsForState.count()).toBeGreaterThan(2);
    }

    const timeElements = firstRow.locator("time");

    if ((await timeElements.count()) > 0) {
      await expect(timeElements.first()).toBeVisible();
    } else {
      const dateCells = firstRow.getByRole("cell");
      const dateCellCount = await dateCells.count();

      expect(dateCellCount).toBeGreaterThan(3);
    }
  }

  /**
   * Verify that task instances exist in the table
   */
  public async verifyTaskInstancesExist(): Promise<void> {
    // Skip the header row (index 0); all subsequent rows are data rows.
    const rows = this.taskInstancesTable
      .getByRole("row")
      .filter({ hasNot: this.taskInstancesTable.getByRole("columnheader") });

    expect(await rows.count()).toBeGreaterThan(0);
  }
}
