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
  public readonly taskNameRows: Locator;

  public constructor(page: Page) {
    super(page);
    this.gridViewButton = page.getByTestId("grid-view-button");
    this.gridCells = page.getByTestId(/^grid-(?!view-button).+/);
    this.taskNameRows = page.getByTestId(/^task-(?!state-badge).+/);
  }

  public async clickGridCellAndVerifyDetails(): Promise<void> {
    await this.waitForGridToLoad();

    const firstCell = this.gridCells.first();

    await expect(firstCell).toBeVisible();
    await firstCell.click();
    await expect(this.page).toHaveURL(/.*\/tasks\/.*/, { timeout: 15_000 });
    await expect(this.page.getByTestId("virtualized-list")).toBeVisible({ timeout: 10_000 });
  }

  public async navigateToDag(dagId: string): Promise<void> {
    await expect(async () => {
      await this.navigateTo(`/dags/${dagId}`);
      await expect(this.page).toHaveURL(new RegExp(`/dags/${dagId}`), { timeout: 5000 });
      await expect(this.gridViewButton).toBeVisible({ timeout: 5000 });
    }).toPass({ intervals: [2000], timeout: 60_000 });
  }

  public async switchToGridView(): Promise<void> {
    await expect(this.gridViewButton).toBeVisible({ timeout: 10_000 });
    await this.gridViewButton.click();
    await this.waitForGridToLoad();
  }

  public async verifyGridHasTaskInstances(): Promise<void> {
    await this.waitForGridToLoad();
    await expect(this.taskNameRows).not.toHaveCount(0);
    await expect(this.gridCells).not.toHaveCount(0);
  }

  public async verifyGridViewIsActive(): Promise<void> {
    await expect(this.gridViewButton).toBeVisible({ timeout: 10_000 });
    await expect(this.gridCells.first()).toBeVisible({ timeout: 15_000 });
    await expect(this.taskNameRows.first()).toBeVisible({ timeout: 20_000 });
  }

  public async verifyTaskStatesAreColorCoded(): Promise<void> {
    await this.waitForGridToLoad();

    const firstCell = this.gridCells.first();

    await expect(firstCell).toBeVisible();

    const badge = firstCell.getByTestId("task-state-badge");

    await expect(badge).toBeVisible();
    await expect(badge).not.toHaveCSS("background-color", "rgba(0, 0, 0, 0)");
  }

  public async verifyTaskTooltipOnHover(): Promise<void> {
    await this.waitForGridToLoad();

    const firstCell = this.gridCells.first();

    await expect(firstCell).toBeVisible();
    await firstCell.hover();

    await expect(this.page.getByRole("tooltip").first()).toBeVisible({ timeout: 10_000 });
  }

  public async waitForGridToLoad(): Promise<void> {
    await expect(this.gridCells.first()).toBeVisible({ timeout: 20_000 });
    await expect(this.taskNameRows.first()).toBeVisible({ timeout: 10_000 });
  }
}
