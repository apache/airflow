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
    this.taskInstancesTable = page.locator('table, div[role="table"]');
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
   * Verify pagination controls and navigation
   */
  public async verifyPagination(limit: number): Promise<void> {
    await this.navigateTo(`${TaskInstancesPage.taskInstancesUrl}?offset=0&limit=${limit}`);
    await this.page.waitForURL(/.*limit=/, { timeout: 10_000 });
    await this.page.waitForLoadState("networkidle");
    await this.taskInstancesTable.waitFor({ state: "visible", timeout: 10_000 });

    const dataLinks = this.taskInstancesTable.locator("a[href*='/dags/']");

    await expect(dataLinks.first()).toBeVisible({ timeout: 30_000 });

    const rows = this.taskInstancesTable.locator('tbody tr:not(.no-data), div[role="row"]:not(:first-child)');

    expect(await rows.count()).toBeGreaterThan(0);

    const paginationNav = this.page.locator('nav[aria-label="pagination"], [role="navigation"]');

    await expect(paginationNav.first()).toBeVisible({ timeout: 10_000 });

    const page1Button = this.page.getByRole("button", { name: /page 1|^1$/ });

    await expect(page1Button.first()).toBeVisible({ timeout: 5000 });

    const page2Button = this.page.getByRole("button", { name: /page 2|^2$/ });
    const hasPage2 = await page2Button
      .first()
      .isVisible()
      .catch(() => false);

    if (hasPage2) {
      await page2Button.first().click();
      await this.page.waitForLoadState("networkidle");
      await this.taskInstancesTable.waitFor({ state: "visible", timeout: 10_000 });

      const dataLinksPage2 = this.taskInstancesTable.locator("a[href*='/dags/']");
      const noDataMessage = this.page.locator("text=/no.*data|no.*task instances|no.*results/i");

      await expect(dataLinksPage2.first().or(noDataMessage.first())).toBeVisible({ timeout: 30_000 });
    }
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

    if (countAfter === 0) {
      return;
    }

    const badgeCount = await stateBadges.count();

    expect(badgeCount).toBeGreaterThan(0);

    for (let i = 0; i < Math.min(badgeCount, 20); i++) {
      const badge = stateBadges.nth(i);
      const badgeText = (await badge.textContent())?.trim().toLowerCase();

      expect(badgeText).toContain(expectedState.toLowerCase());
    }
  }

  /**
   * Verify that different task states are visually distinct (success and failed)
   */
  public async verifyStateVisualDistinction(): Promise<void> {
    const firstDataRow = this.taskInstancesTable
      .locator('tbody tr:not(.no-data), div[role="row"]:not(:first-child)')
      .first();

    await expect(firstDataRow).toBeVisible({ timeout: 10_000 });

    const cellWithContent = firstDataRow.locator('td, div[role="cell"]').filter({ hasText: /.+/ });

    await expect(cellWithContent.first()).toBeVisible({ timeout: 10_000 });

    const stateBadges = this.taskInstancesTable.locator('[class*="badge"], [class*="Badge"]');

    await stateBadges.first().waitFor({ state: "visible", timeout: 10_000 });

    const badgeCount = await stateBadges.count();

    expect(badgeCount).toBeGreaterThan(0);

    const stateStyles = new Map<string, string>();
    const limit = Math.min(badgeCount, 50);

    for (let i = 0; i < limit; i++) {
      const badge = stateBadges.nth(i);
      const text = (await badge.textContent())?.trim().toLowerCase();
      const bgColor = await badge.evaluate((el) => window.getComputedStyle(el).backgroundColor);

      if (text !== "" && text !== undefined) {
        stateStyles.set(text, bgColor);
      }
    }

    expect(stateStyles.size).toBeGreaterThanOrEqual(1);

    const requiredStates = ["success", "failed"];
    const foundStates = [...stateStyles.keys()];
    const missingStates: Array<string> = [];

    requiredStates.forEach((requiredState) => {
      const stateFound = foundStates.some((foundState) => foundState.includes(requiredState));

      if (!stateFound) {
        missingStates.push(requiredState);
      }
    });

    expect(
      missingStates.length,
      `Missing required states: ${missingStates.join(", ")}. Found states: ${foundStates.join(", ")}`,
    ).toBe(0);

    const allColors = [...stateStyles.values()];

    allColors.forEach((color) => {
      expect(color).toBeTruthy();
      expect(color).not.toBe("rgba(0, 0, 0, 0)");
    });

    const uniqueColors = new Set(allColors);

    expect(uniqueColors.size).toBeGreaterThanOrEqual(2);

    const stateColors = new Map<string, string>();

    requiredStates.forEach((state) => {
      const matchingState = foundStates.find((foundState) => foundState.includes(state));

      if (matchingState !== undefined) {
        const color = stateStyles.get(matchingState);

        if (color !== undefined) {
          stateColors.set(state, color);
        }
      }
    });

    expect(stateColors.size).toBe(requiredStates.length);

    const successColor = stateColors.get("success");
    const failedColor = stateColors.get("failed");

    expect(successColor).toBeTruthy();
    expect(failedColor).toBeTruthy();
    expect(successColor).not.toBe(failedColor);
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
}
