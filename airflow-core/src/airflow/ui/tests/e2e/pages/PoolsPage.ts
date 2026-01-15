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

export type PoolFormValues = {
  description?: string;
  includeDeferred?: boolean;
  name: string;
  slots: number;
};

export class PoolsPage extends BasePage {
  public static get poolsUrl(): string {
    return "/pools";
  }

  public readonly addPoolButton: Locator;
  public readonly dialogCloseButton: Locator;
  public readonly dialogTitle: Locator;
  public readonly paginationNextButton: Locator;
  public readonly paginationPrevButton: Locator;
  public readonly poolCards: Locator;
  public readonly poolDescriptionInput: Locator;
  public readonly poolIncludeDeferredCheckbox: Locator;
  public readonly poolNameInput: Locator;
  public readonly poolSlotsInput: Locator;
  public readonly resetButton: Locator;
  public readonly saveButton: Locator;
  public readonly searchBar: Locator;
  public readonly sortDropdown: Locator;

  public constructor(page: Page) {
    super(page);
    this.searchBar = page.getByPlaceholder(/search pools/i);
    this.addPoolButton = page.locator('button:has-text("Add Pool")');
    this.sortDropdown = page.locator('[data-part="trigger"]:has-text("A-Z"), [data-part="trigger"]:has-text("Z-A")');
    this.poolCards = page.locator('[data-testid="card-list"] > div, [data-testid="data-table"] [data-testid^="card-"]');
    this.paginationNextButton = page.locator('[data-testid="next"]');
    this.paginationPrevButton = page.locator('[data-testid="prev"]');
    this.dialogTitle = page.locator('[data-scope="dialog"] h2, .chakra-dialog h2, [role="dialog"] h2');
    this.dialogCloseButton = page.locator('[data-scope="dialog"] button[aria-label="Close"], .chakra-dialog button[aria-label="Close"]');
    this.poolNameInput = page.locator('input[name="name"]');
    this.poolSlotsInput = page.locator('input[name="slots"], input[type="number"]');
    this.poolDescriptionInput = page.locator('textarea[name="description"], textarea');
    this.poolIncludeDeferredCheckbox = page.locator('input[type="checkbox"], [role="checkbox"]');
    this.saveButton = page.locator('button:has-text("Save")');
    this.resetButton = page.locator('button:has-text("Reset")');
  }

  /**
   * Create a new pool
   */
  public async createPool(pool: PoolFormValues): Promise<void> {
    await this.addPoolButton.click();
    await expect(this.dialogTitle).toBeVisible({ timeout: 10_000 });

    await this.poolNameInput.fill(pool.name);
    await this.poolSlotsInput.fill(String(pool.slots));

    if (pool.description !== undefined && pool.description !== "") {
      await this.poolDescriptionInput.fill(pool.description);
    }

    if (pool.includeDeferred) {
      await this.poolIncludeDeferredCheckbox.click();
    }

    await this.saveButton.click();
    await this.waitForDialogToClose();
  }

  /**
   * Delete a pool by name
   */
  public async deletePool(poolName: string): Promise<void> {
    const poolCard = await this.findPoolCardByName(poolName);
    const deleteButton = poolCard.locator('button[aria-label*="Delete"], button:has([data-icon="trash"])');

    await deleteButton.click();

    // Confirm deletion in dialog
    const confirmDeleteButton = this.page.locator('[role="dialog"] button:has-text("Delete")');

    await expect(confirmDeleteButton).toBeVisible({ timeout: 5000 });
    await confirmDeleteButton.click();

    await this.waitForDialogToClose();
  }

  /**
   * Edit a pool's slots
   */
  public async editPoolSlots(poolName: string, newSlots: number): Promise<void> {
    const poolCard = await this.findPoolCardByName(poolName);
    const editButton = poolCard.locator('button[aria-label*="Edit"], button:has([data-icon="pencil"])');

    await editButton.click();
    await expect(this.dialogTitle).toBeVisible({ timeout: 10_000 });

    await this.poolSlotsInput.clear();
    await this.poolSlotsInput.fill(String(newSlots));

    await this.saveButton.click();
    await this.waitForDialogToClose();
  }

  /**
   * Find a pool card by name
   */
  public async findPoolCardByName(poolName: string): Promise<Locator> {
    await this.waitForPoolsLoaded();

    // Pool cards show the name in bold text with format "name (X slots)"
    const poolCard = this.page.locator(`div:has(> div > div > div:has-text("${poolName}"))`).first();

    // Try a broader match if specific one doesn't work
    const fallbackCard = this.page.locator(`text=${poolName}`).locator("xpath=ancestor::div[contains(@class, 'chakra')]").first();

    const targetCard = await poolCard.count() > 0 ? poolCard : fallbackCard;

    await expect(targetCard).toBeVisible({ timeout: 10_000 });

    return targetCard;
  }

  /**
   * Get all pool names currently visible
   */
  public async getPoolNames(): Promise<Array<string>> {
    await this.waitForPoolsLoaded();

    const cards = this.page.locator('div[class*="border"]').filter({ hasText: /slots/i });
    const count = await cards.count();
    const names: Array<string> = [];

    for (let i = 0; i < count; i++) {
      const card = cards.nth(i);
      const nameText = await card.locator("text >> nth=0").textContent();

      if (nameText !== null && nameText !== "") {
        // Extract pool name from "name (X slots)" format
        const regex = /^(.+?)\s*\(/;
        const match = regex.exec(nameText);

        if (match?.[1] !== undefined && match[1] !== "") {
          names.push(match[1].trim());
        }
      }
    }

    return names;
  }

  /**
   * Get the slot count for a specific pool
   */
  public async getPoolSlots(poolName: string): Promise<number> {
    const poolCard = await this.findPoolCardByName(poolName);
    const nameText = await poolCard.locator('text:has-text("slots")').first().textContent();

    if (nameText !== null && nameText !== "") {
      const regex = /\((\d+)\s+slots?\)/i;
      const match = regex.exec(nameText);

      if (match?.[1] !== undefined && match[1] !== "") {
        return parseInt(match[1], 10);
      }
    }

    return -1;
  }

  /**
   * Check if next page is available
   */
  public async hasNextPage(): Promise<boolean> {
    const count = await this.paginationNextButton.count();

    if (count === 0) {
      return false;
    }

    return this.paginationNextButton.isEnabled();
  }

  /**
   * Check if a pool exists in the list
   */
  public async hasPool(poolName: string): Promise<boolean> {
    await this.waitForPoolsLoaded();

    const poolText = this.page.locator(`text=${poolName}`);
    const count = await poolText.count();

    return count > 0;
  }

  /**
   * Navigate to pools page
   */
  public async navigate(): Promise<void> {
    await this.navigateTo(PoolsPage.poolsUrl);
    await this.waitForPoolsLoaded();
  }

  /**
   * Navigate to next page
   */
  public async nextPage(): Promise<void> {
    await this.paginationNextButton.click();
    await this.waitForPoolsLoaded();
  }

  /**
   * Navigate to previous page
   */
  public async prevPage(): Promise<void> {
    await this.paginationPrevButton.click();
    await this.waitForPoolsLoaded();
  }

  /**
   * Search for pools by name pattern
   */
  public async searchPools(namePattern: string): Promise<void> {
    await this.searchBar.clear();
    await this.searchBar.fill(namePattern);
    await this.page.waitForTimeout(500); // Debounce
    await this.waitForPoolsLoaded();
  }

  /**
   * Change sort order
   */
  public async sortPools(order: "asc" | "desc"): Promise<void> {
    await this.sortDropdown.click();

    const optionText = order === "asc" ? /A-Z/ : /Z-A/;
    const sortOption = this.page.locator(`[data-part="item"]:has-text("${order === "asc" ? "A-Z" : "Z-A"}")`);

    await sortOption.click();
    await this.waitForPoolsLoaded();
  }

  /**
   * Verify pool usage bar is displayed
   */
  public async verifyPoolUsageDisplayed(poolName: string): Promise<boolean> {
    const poolCard = await this.findPoolCardByName(poolName);

    // Look for the pool bar component inside the card
    const poolBar = poolCard.locator('div[class*="pool"], [data-testid*="pool-bar"]');

    return (await poolBar.count()) > 0;
  }

  /**
   * Wait for dialog to close
   */
  public async waitForDialogToClose(): Promise<void> {
    const dialog = this.page.locator('[role="dialog"], [data-scope="dialog"]');

    await expect(dialog).not.toBeVisible({ timeout: 10_000 });
  }

  /**
   * Wait for pools to finish loading
   */
  public async waitForPoolsLoaded(): Promise<void> {
    // Wait for any loading skeletons to disappear
    const skeleton = this.page.locator('[data-scope="skeleton"]');

    await expect(skeleton).not.toBeVisible({ timeout: 30_000 }).catch(() => {
      // Skeleton may not be present, which is fine
    });

    // Wait for either pool cards to be visible or "no pools" message
    const poolsOrEmpty = this.page
      .locator('text=/slots/i')
      .or(this.page.locator('text=/no pools/i'));

    await expect(poolsOrEmpty.first()).toBeVisible({ timeout: 30_000 });
  }
}
