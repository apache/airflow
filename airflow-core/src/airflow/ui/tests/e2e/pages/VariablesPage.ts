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

/**
 * Variables Page Object
 */
export class VariablesPage extends BasePage {
  // Page URL
  public static get variablesUrl(): string {
    return "/variables";
  }

  // Locators
  public readonly addVariableButton: Locator;
  public readonly importVariablesButton: Locator;
  public readonly searchBox: Locator;
  public readonly paginationNextButton: Locator;
  public readonly paginationPrevButton: Locator;
  public readonly dialogContent: Locator;
  public readonly keyInput: Locator;
  public readonly valueTextarea: Locator;
  public readonly descriptionTextarea: Locator;
  public readonly saveButton: Locator;
  public readonly resetButton: Locator;
  public readonly deleteButton: Locator;
  public readonly confirmDeleteButton: Locator;
  public readonly fileInput: Locator;

  public constructor(page: Page) {
    super(page);
    this.addVariableButton = page.getByRole("button", { name: /add variable/i });
    this.importVariablesButton = page.getByRole("button", { name: /import variables/i });
    this.searchBox = page.getByRole("textbox", { name: /search/i });
    this.paginationNextButton = page.locator('[data-testid="next"]');
    this.paginationPrevButton = page.locator('[data-testid="prev"]');
    this.dialogContent = page.locator('[role="dialog"]');
    this.keyInput = page.locator('input[name="key"]');
    this.valueTextarea = page.locator('textarea[name="value"]');
    this.descriptionTextarea = page.locator('textarea[name="description"]');
    this.saveButton = page.getByRole("button", { name: /save/i });
    this.resetButton = page.getByRole("button", { name: /reset/i });
    this.deleteButton = page.getByRole("button", { name: /delete/i });
    this.confirmDeleteButton = page.getByRole("button", { name: /^delete$/i }).last();
    this.fileInput = page.locator('input[type="file"]');
  }

  /**
   * Navigate to Variables page
   */
  public async navigate(): Promise<void> {
    await this.navigateTo(VariablesPage.variablesUrl);
    await this.waitForVariablesList();
  }

  /**
   * Wait for variables list to be rendered or show empty state
   */
  public async waitForVariablesList(): Promise<void> {
    // Wait for either the table to load or the "no variables" message
    await this.page.waitForSelector('[data-testid="data-table"], [data-testid="no-rows-message"]', {
      timeout: 10_000,
    });
  }

  /**
   * Get all variable keys from the current page
   */
  public async getVariableKeys(): Promise<Array<string>> {
    const cells = this.page.locator("table tbody tr td:nth-child(2)");
    const texts = await cells.allTextContents();

    return texts.map((text) => text.trim()).filter((text) => text !== "");
  }

  /**
   * Get the count of variables displayed
   */
  public async getVariableCount(): Promise<number> {
    const keys = await this.getVariableKeys();

    return keys.length;
  }

  /**
   * Check if a variable exists by key
   */
  public async variableExists(key: string): Promise<boolean> {
    const keys = await this.getVariableKeys();

    return keys.includes(key);
  }

  /**
   * Search for variables
   */
  public async searchVariables(searchTerm: string): Promise<void> {
    await this.searchBox.clear();
    await this.searchBox.fill(searchTerm);
    await this.page.waitForTimeout(500);
    await this.waitForVariablesList();
  }

  /**
   * Clear search
   */
  public async clearSearch(): Promise<void> {
    await this.searchBox.clear();
    await this.page.waitForTimeout(500);
    await this.waitForVariablesList();
  }

  /**
   * Click next page button
   */
  public async clickNextPage(): Promise<void> {
    const initialKeys = await this.getVariableKeys();

    await this.paginationNextButton.click();

    await expect.poll(() => this.getVariableKeys(), { timeout: 10_000 }).not.toEqual(initialKeys);
    await this.waitForVariablesList();
  }

  /**
   * Click previous page button
   */
  public async clickPrevPage(): Promise<void> {
    const initialKeys = await this.getVariableKeys();

    await this.paginationPrevButton.click();

    await expect.poll(() => this.getVariableKeys(), { timeout: 10_000 }).not.toEqual(initialKeys);
    await this.waitForVariablesList();
  }

  /**
   * Open add variable dialog
   */
  public async openAddVariableDialog(): Promise<void> {
    await this.addVariableButton.click();
    await expect(this.dialogContent).toBeVisible({ timeout: 5000 });
  }

  /**
   * Create a new variable
   */
  public async createVariable(key: string, value: string, description?: string): Promise<void> {
    await this.openAddVariableDialog();

    await this.keyInput.fill(key);
    await this.valueTextarea.fill(value);

    if (description) {
      await this.descriptionTextarea.fill(description);
    }

    await this.saveButton.click();

    // Wait for dialog to close
    await expect(this.dialogContent).not.toBeVisible({ timeout: 10_000 });

    // Wait for the variable to appear in the list
    await this.page.waitForTimeout(1000);
    await this.waitForVariablesList();
  }

  /**
   * Get the edit button for a specific variable
   */
  public getEditButtonForVariable(key: string): Locator {
    const row = this.page.locator("table tbody tr").filter({ hasText: key });

    return row.getByRole("button", { name: /edit/i });
  }

  /**
   * Edit an existing variable
   */
  public async editVariable(key: string, newValue: string, newDescription?: string): Promise<void> {
    const editButton = this.getEditButtonForVariable(key);

    await editButton.click();
    await expect(this.dialogContent).toBeVisible({ timeout: 5000 });

    await this.valueTextarea.clear();
    await this.valueTextarea.fill(newValue);

    if (newDescription !== undefined) {
      await this.descriptionTextarea.clear();
      await this.descriptionTextarea.fill(newDescription);
    }

    await this.saveButton.click();

    // Wait for dialog to close
    await expect(this.dialogContent).not.toBeVisible({ timeout: 10_000 });

    // Wait for the update to reflect
    await this.page.waitForTimeout(1000);
    await this.waitForVariablesList();
  }

  /**
   * Get the delete button for a specific variable
   */
  public getDeleteButtonForVariable(key: string): Locator {
    const row = this.page.locator("table tbody tr").filter({ hasText: key });

    return row.getByRole("button", { name: /delete/i });
  }

  /**
   * Delete a variable by key
   */
  public async deleteVariable(key: string): Promise<void> {
    const deleteButton = this.getDeleteButtonForVariable(key);

    await deleteButton.click();

    // Confirm deletion
    await this.confirmDeleteButton.click();

    // Wait for the variable to be removed
    await this.page.waitForTimeout(1000);
    await this.waitForVariablesList();
  }

  /**
   * Open import variables dialog
   */
  public async openImportVariablesDialog(): Promise<void> {
    await this.importVariablesButton.click();
    await expect(this.dialogContent).toBeVisible({ timeout: 5000 });
  }

  /**
   * Import variables from a JSON string
   */
  public async importVariablesFromJson(jsonContent: string): Promise<void> {
    await this.openImportVariablesDialog();

    // Create a temporary file and upload it
    const buffer = Buffer.from(jsonContent);

    await this.fileInput.setInputFiles({
      buffer,
      mimeType: "application/json",
      name: "variables.json",
    });

    // Click the import/upload button
    const uploadButton = this.dialogContent.getByRole("button", { name: /import|upload/i });

    await uploadButton.click();

    // Wait for dialog to close
    await expect(this.dialogContent).not.toBeVisible({ timeout: 10_000 });

    // Wait for the import to complete
    await this.page.waitForTimeout(1000);
    await this.waitForVariablesList();
  }

  /**
   * Select a variable by clicking its checkbox
   */
  public async selectVariable(key: string): Promise<void> {
    const row = this.page.locator("table tbody tr").filter({ hasText: key });
    const checkbox = row.locator('input[type="checkbox"]');

    await checkbox.click();
  }

  /**
   * Select all variables
   */
  public async selectAllVariables(): Promise<void> {
    const headerCheckbox = this.page.locator('table thead th:first-child input[type="checkbox"]');

    await headerCheckbox.click();
  }

  /**
   * Delete selected variables using bulk delete
   */
  public async deleteSelectedVariables(): Promise<void> {
    // The action bar should appear when variables are selected
    const actionBarDeleteButton = this.page.locator('[data-testid="action-bar"]').getByRole("button", {
      name: /delete/i,
    });

    await actionBarDeleteButton.click();
    await this.confirmDeleteButton.click();

    // Wait for deletion to complete
    await this.page.waitForTimeout(1000);
    await this.waitForVariablesList();
  }

  /**
   * Get the value displayed for a variable
   */
  public async getVariableValue(key: string): Promise<string> {
    const row = this.page.locator("table tbody tr").filter({ hasText: key });
    const valueCell = row.locator("td:nth-child(3)");

    return (await valueCell.textContent()) ?? "";
  }

  /**
   * Get the description displayed for a variable
   */
  public async getVariableDescription(key: string): Promise<string> {
    const row = this.page.locator("table tbody tr").filter({ hasText: key });
    const descCell = row.locator("td:nth-child(4)");

    return (await descCell.textContent()) ?? "";
  }

  /**
   * Check if pagination is available
   */
  public async isPaginationAvailable(): Promise<boolean> {
    const nextVisible = await this.paginationNextButton.isVisible();
    const prevVisible = await this.paginationPrevButton.isVisible();

    return nextVisible && prevVisible;
  }

  /**
   * Get column headers
   */
  public async getColumnHeaders(): Promise<Array<string>> {
    const headers = this.page.locator("table thead th");
    const texts = await headers.allTextContents();

    return texts.map((text) => text.trim()).filter((text) => text !== "");
  }
}
