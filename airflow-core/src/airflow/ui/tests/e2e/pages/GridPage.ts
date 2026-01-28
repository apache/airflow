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

export class GridPage extends BasePage {
  public readonly gridCells: Locator;
  public readonly gridViewButton: Locator;
  public readonly taskNameLinks: Locator;

  public constructor(page: Page) {
    super(page);
    this.gridViewButton = page.getByTestId("grid-view-button");
    this.gridCells = page.locator('a[id^="grid-"]');
    this.taskNameLinks = page.locator('a[href*="/tasks/"]');
  }

  public async clickGridCellAndVerifyDetails(): Promise<void> {
    await this.waitForGridToLoad();

    const firstCell = this.gridCells.first();

    await expect(firstCell).toBeVisible();
    await firstCell.click();
    await this.page.waitForURL(/.*\/tasks\/.*/, { timeout: 15_000 });
    await expect(this.page.getByTestId("virtualized-list")).toBeVisible({ timeout: 10_000 });
  }

  public async getGridCellCount(): Promise<number> {
    await this.waitForGridToLoad();

    return this.gridCells.count();
  }

  public async getTaskNames(): Promise<Array<string>> {
    await this.waitForGridToLoad();

    const names = await this.taskNameLinks.allTextContents();
    const uniqueNames = [...new Set(names.map((name) => name.trim()).filter((name) => name !== ""))];

    return uniqueNames;
  }

  public async navigateToDag(dagId: string): Promise<void> {
    await this.navigateTo(`/dags/${dagId}`);
    await this.page.waitForURL(`**/dags/${dagId}**`, { timeout: 15_000 });
    await expect(this.gridViewButton).toBeVisible({ timeout: 10_000 });
  }

  public async switchToGridView(): Promise<void> {
    await expect(this.gridViewButton).toBeVisible({ timeout: 10_000 });
    await this.gridViewButton.click();
    await this.waitForGridToLoad();
  }

  public async verifyGridViewIsActive(): Promise<void> {
    await expect(this.gridViewButton).toBeVisible({ timeout: 10_000 });
    await expect(this.gridCells.first()).toBeVisible({ timeout: 15_000 });
    await expect(this.taskNameLinks.first()).toBeVisible({ timeout: 10_000 });
  }

  public async verifyTaskStatesAreColorCoded(): Promise<void> {
    await this.waitForGridToLoad();

    const firstCell = this.gridCells.first();

    await expect(firstCell).toBeVisible();

    const badge = firstCell.getByTestId("task-state-badge");

    await expect(badge).toBeVisible();

    const bgColor = await badge.evaluate((el) => window.getComputedStyle(el).backgroundColor);

    const isTransparent = !bgColor || bgColor === "transparent" || bgColor === "rgba(0, 0, 0, 0)";

    expect(isTransparent).toBe(false);
  }

  public async verifyTaskTooltipOnHover(): Promise<void> {
    await this.waitForGridToLoad();

    const firstCell = this.gridCells.first();

    await expect(firstCell).toBeVisible();
    await firstCell.hover();

    const tooltip = this.page.locator('[role="tooltip"], [data-scope="tooltip"]');

    await expect(tooltip.first()).toBeVisible({ timeout: 10_000 });
  }

  public async waitForGridToLoad(): Promise<void> {
    await expect(this.gridCells.first()).toBeVisible({ timeout: 20_000 });
    await expect(this.taskNameLinks.first()).toBeVisible({ timeout: 10_000 });
  }
}
