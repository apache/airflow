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

export class PoolsPage extends BasePage {
  public static get poolsUrl(): string {
    return "/pools";
  }

  public readonly addPoolButton: Locator;
  public readonly cardList: Locator;
  public readonly searchInput: Locator;

  public constructor(page: Page) {
    super(page);
    this.addPoolButton = page.getByRole("button", { name: "Add Pool" });
    this.cardList = page.locator('[data-testid="card-list"]');
    this.searchInput = page.getByPlaceholder("Search Pools");
  }

  public async createPool(name: string, slots: number, description?: string): Promise<void> {
    await this.addPoolButton.click();

    const dialog = this.page.getByRole("dialog");

    await expect(dialog).toBeVisible({ timeout: 10_000 });

    const nameInput = dialog.locator('input[name="name"]');

    await nameInput.fill(name);

    const slotsInput = dialog.locator('input[type="number"]');

    await slotsInput.fill("");
    await slotsInput.fill(String(slots));

    if (description !== undefined && description !== "") {
      const descriptionInput = dialog.locator("textarea");

      await descriptionInput.fill(description);
    }

    const saveButton = dialog.getByRole("button", { name: "Save" });

    await expect(saveButton).toBeEnabled({ timeout: 5000 });

    const responsePromise = this.page.waitForResponse(
      (response) => response.url().includes("/api/v2/pools") && response.request().method() === "POST",
      { timeout: 10_000 },
    );

    await saveButton.click();
    await responsePromise;
    await this.page.waitForLoadState("networkidle");
  }

  public async deletePool(poolName: string): Promise<void> {
    const poolCard = this.getPoolCard(poolName);
    const deleteButton = poolCard.getByRole("button", { name: "Delete Pool" });

    await deleteButton.click();

    const confirmDialog = this.page.getByRole("dialog");

    await expect(confirmDialog).toBeVisible({ timeout: 10_000 });

    const confirmDeleteButton = confirmDialog.getByRole("button", { name: "Delete" });

    const responsePromise = this.page.waitForResponse(
      (response) => response.url().includes("/api/v2/pools") && response.request().method() === "DELETE",
      { timeout: 10_000 },
    );

    await confirmDeleteButton.click();
    await responsePromise;
    await this.page.waitForLoadState("networkidle");
  }

  public async editPoolSlots(poolName: string, newSlots: number): Promise<void> {
    const poolCard = this.getPoolCard(poolName);
    const editButton = poolCard.getByRole("button", { name: "Edit Pool" });

    await editButton.click();

    const dialog = this.page.getByRole("dialog");

    await expect(dialog).toBeVisible({ timeout: 10_000 });

    const slotsInput = dialog.locator('input[type="number"]');

    await slotsInput.fill("");
    await slotsInput.fill(String(newSlots));

    const saveButton = dialog.getByRole("button", { name: "Save" });

    await expect(saveButton).toBeEnabled({ timeout: 5000 });

    const responsePromise = this.page.waitForResponse(
      (response) => response.url().includes("/api/v2/pools") && response.request().method() === "PATCH",
      { timeout: 10_000 },
    );

    await saveButton.click();
    await responsePromise;
    await this.page.waitForLoadState("networkidle");
  }

  public getPoolCard(poolName: string): Locator {
    return this.cardList.locator("div").filter({ hasText: poolName }).first();
  }

  public async navigate(): Promise<void> {
    await this.navigateTo(PoolsPage.poolsUrl);
    await this.page.waitForURL("**/pools", { timeout: 15_000 });
    await this.page.waitForLoadState("networkidle");
  }

  public async verifyPoolExists(poolName: string): Promise<void> {
    await this.page.waitForLoadState("networkidle");

    const poolText = this.cardList.getByText(poolName, { exact: false });

    await expect(poolText.first()).toBeVisible({ timeout: 10_000 });
  }

  public async verifyPoolNotExists(poolName: string): Promise<void> {
    await this.page.waitForLoadState("networkidle");
    await this.page.waitForTimeout(1000);

    const poolText = this.cardList.getByText(poolName, { exact: true });

    await expect(poolText).toBeHidden({ timeout: 10_000 });
  }

  public async verifyPoolsListDisplays(): Promise<void> {
    await expect(this.addPoolButton).toBeVisible({ timeout: 10_000 });
    await expect(this.cardList).toBeVisible({ timeout: 10_000 });

    const defaultPoolCard = this.cardList.getByText("default_pool", { exact: false });

    await expect(defaultPoolCard.first()).toBeVisible({ timeout: 10_000 });
  }

  public async verifyPoolSlots(poolName: string, expectedSlots: number): Promise<void> {
    await this.page.waitForLoadState("networkidle");

    const slotsText = this.cardList.getByText(`${poolName} (${expectedSlots} Slots)`, { exact: false });

    await expect(slotsText.first()).toBeVisible({ timeout: 10_000 });
  }

  public async verifyPoolUsageDisplays(poolName: string): Promise<void> {
    const poolCard = this.getPoolCard(poolName);

    await expect(poolCard).toBeVisible({ timeout: 10_000 });

    const slotsText = poolCard.getByText("Slots", { exact: false });

    await expect(slotsText.first()).toBeVisible({ timeout: 10_000 });
  }
}
