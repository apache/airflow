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

import type { Page } from "@playwright/test";
import { expect } from "@playwright/test";

import { BasePage } from "./BasePage";
import { testConfig } from "../testConfig";

interface VariableData {
    key: string;
    value: string;
    description?: string;
    isEncrypted?: boolean;
}

export class VariablesPage extends BasePage {
    // Locators
    private searchInput = 'input[placeholder="Search Keys"]';
    private addButton = 'button:has-text("Add")';
    private importButton = 'button:has-text("Import")';
    private dataTable = '[role="table"]';
    private tableRow = '[role="row"]';
    private tableHeader = '[role="columnheader"]';
    private actionBar = '[data-scope="action-bar"]';

    // Dialog locators
    private dialog = '[role="dialog"]';
    private dialogKeyInput = 'input[name="key"]';
    private dialogValueInput = 'textarea[name="value"]';
    private dialogDescriptionInput = 'textarea[name="description"]';
    private dialogEncryptedCheckbox = 'input[type="checkbox"][name="is_encrypted"]';
    private dialogSubmitButton = 'button[type="submit"]';
    private dialogCancelButton = 'button:has-text("Cancel")';

    // Confirmation dialog
    private confirmDeleteButton = 'button:has-text("Yes, Delete")';

    // File input for import
    private fileInput = 'input[type="file"]';

    constructor(page: Page) {
        super(page);
    }

    async goto(): Promise<void> {
        await this.navigateTo(testConfig.paths.variables);
        await this.wait(1000); // Wait for page to stabilize
    }

    // Search functionality
    async searchVariables(pattern: string): Promise<void> {
        await this.fill(this.searchInput, pattern);
        await this.wait(500); // Wait for search to apply
    }

    async clearSearch(): Promise<void> {
        await this.fill(this.searchInput, "");
        await this.wait(500);
    }

    // Create variable
    async clickAddVariable(): Promise<void> {
        await this.click(this.addButton);
        await this.waitForElement(this.dialog);
    }

    async createVariable(
        key: string,
        value: string,
        description: string = "",
        isEncrypted: boolean = false,
    ): Promise<void> {
        await this.clickAddVariable();

        // Fill form
        await this.fill(this.dialogKeyInput, key);
        await this.fill(this.dialogValueInput, value);
        if (description) {
            await this.fill(this.dialogDescriptionInput, description);
        }
        if (isEncrypted) {
            await this.click(this.dialogEncryptedCheckbox);
        }

        // Submit
        await this.click(this.dialogSubmitButton);
        await this.wait(1000); // Wait for variable to be created
    }

    // Edit variable
    async clickEditVariable(key: string): Promise<void> {
        const row = await this.getVariableRow(key);
        if (!row) {
            throw new Error(`Variable with key "${key}" not found`);
        }

        // Find edit button in the row
        const editButton = await row.$('button[aria-label*="edit"], button:has-text("Edit")');
        if (!editButton) {
            throw new Error(`Edit button not found for variable "${key}"`);
        }

        await editButton.click();
        await this.waitForElement(this.dialog);
    }

    async editVariable(
        key: string,
        newValue?: string,
        newDescription?: string,
        newIsEncrypted?: boolean,
    ): Promise<void> {
        await this.clickEditVariable(key);

        // Update fields
        if (newValue !== undefined) {
            await this.fill(this.dialogValueInput, newValue);
        }
        if (newDescription !== undefined) {
            await this.fill(this.dialogDescriptionInput, newDescription);
        }
        if (newIsEncrypted !== undefined) {
            const checkbox = await this.page.$(this.dialogEncryptedCheckbox);
            const isChecked = await checkbox?.isChecked();
            if (isChecked !== newIsEncrypted) {
                await this.click(this.dialogEncryptedCheckbox);
            }
        }

        // Submit
        await this.click(this.dialogSubmitButton);
        await this.wait(1000);
    }

    // Delete variable
    async clickDeleteVariable(key: string): Promise<void> {
        const row = await this.getVariableRow(key);
        if (!row) {
            throw new Error(`Variable with key "${key}" not found`);
        }

        // Find delete button in the row
        const deleteButton = await row.$('button[aria-label*="delete"], button:has-text("Delete")');
        if (!deleteButton) {
            throw new Error(`Delete button not found for variable "${key}"`);
        }

        await deleteButton.click();
        await this.waitForElement(this.confirmDeleteButton);
    }

    async deleteVariable(key: string): Promise<void> {
        await this.clickDeleteVariable(key);
        await this.click(this.confirmDeleteButton);
        await this.wait(1000);
    }

    // Select variables for bulk operations
    async selectVariable(key: string): Promise<void> {
        const row = await this.getVariableRow(key);
        if (!row) {
            throw new Error(`Variable with key "${key}" not found`);
        }

        const checkbox = await row.$('input[type="checkbox"]');
        if (!checkbox) {
            throw new Error(`Checkbox not found for variable "${key}"`);
        }

        await checkbox.click();
    }

    async selectMultipleVariables(keys: string[]): Promise<void> {
        for (const key of keys) {
            await this.selectVariable(key);
        }
    }

    async deleteMultipleVariables(keys: string[]): Promise<void> {
        // Select all variables
        await this.selectMultipleVariables(keys);

        // Wait for action bar to appear
        await this.waitForElement(this.actionBar);

        // Click delete button in action bar
        const deleteButton = await this.page.$(`${this.actionBar} button:has-text("Delete")`);
        if (!deleteButton) {
            throw new Error("Delete button not found in action bar");
        }

        await deleteButton.click();
        await this.waitForElement(this.confirmDeleteButton);
        await this.click(this.confirmDeleteButton);
        await this.wait(1000);
    }

    // Import variables
    async clickImportVariables(): Promise<void> {
        await this.click(this.importButton);
        await this.wait(500);
    }

    async importVariables(filePath: string): Promise<void> {
        await this.clickImportVariables();

        // Upload file
        const fileInputElement = await this.page.$(this.fileInput);
        if (!fileInputElement) {
            throw new Error("File input not found");
        }

        await fileInputElement.setInputFiles(filePath);
        await this.wait(2000); // Wait for import to complete
    }

    // Helper methods
    async getVariableRow(key: string) {
        const rows = await this.page.$$(this.tableRow);

        for (const row of rows) {
            const text = await row.textContent();
            if (text?.includes(key)) {
                return row;
            }
        }

        return null;
    }

    async isVariableVisible(key: string): Promise<boolean> {
        const row = await this.getVariableRow(key);
        return row !== null;
    }

    async getVariableData(key: string): Promise<VariableData | null> {
        const row = await this.getVariableRow(key);
        if (!row) {
            return null;
        }

        const cells = await row.$$('[role="cell"]');
        if (cells.length < 4) {
            return null;
        }

        // Skip first cell (checkbox), then key, value, description, is_encrypted
        const keyText = await cells[1].textContent();
        const valueText = await cells[2].textContent();
        const descriptionText = await cells[3].textContent();
        const isEncryptedText = await cells[4].textContent();

        return {
            key: keyText?.trim() || "",
            value: valueText?.trim() || "",
            description: descriptionText?.trim() || "",
            isEncrypted: isEncryptedText?.toLowerCase().includes("true"),
        };
    }

    // Sorting
    async sortByColumn(columnName: string): Promise<void> {
        const header = await this.page.$(`${this.tableHeader}:has-text("${columnName}")`);
        if (!header) {
            throw new Error(`Column "${columnName}" not found`);
        }

        await header.click();
        await this.wait(500);
    }

    async getSortDirection(columnName: string): Promise<"asc" | "desc" | null> {
        const header = await this.page.$(`${this.tableHeader}:has-text("${columnName}")`);
        if (!header) {
            return null;
        }

        const ariaSort = await header.getAttribute("aria-sort");
        if (ariaSort === "ascending") return "asc";
        if (ariaSort === "descending") return "desc";
        return null;
    }

    // Pagination
    async goToPage(page: number): Promise<void> {
        const pageButton = await this.page.$(`button:has-text("${page}")`);
        if (pageButton) {
            await pageButton.click();
            await this.wait(500);
        }
    }

    async goToNextPage(): Promise<void> {
        const nextButton = await this.page.$('button[aria-label="Go to next page"]');
        if (nextButton) {
            await nextButton.click();
            await this.wait(500);
        }
    }

    async goToPreviousPage(): Promise<void> {
        const prevButton = await this.page.$('button[aria-label="Go to previous page"]');
        if (prevButton) {
            await prevButton.click();
            await this.wait(500);
        }
    }

    async getCurrentPage(): Promise<number> {
        // Try to find current page number in pagination
        const paginationText = await this.page.textContent('[data-scope="pagination"]');
        if (paginationText) {
            const match = paginationText.match(/Page (\d+)/);
            if (match) {
                return parseInt(match[1], 10);
            }
        }
        return 1;
    }

    async getTotalVariables(): Promise<number> {
        const paginationText = await this.page.textContent('[data-scope="pagination"]');
        if (paginationText) {
            const match = paginationText.match(/of (\d+)/);
            if (match) {
                return parseInt(match[1], 10);
            }
        }
        return 0;
    }

    async getDisplayedVariablesCount(): Promise<number> {
        const rows = await this.page.$$(this.tableRow);
        // Subtract 1 for header row
        return Math.max(0, rows.length - 1);
    }

    // Verification helpers
    async verifyVariableExists(
        key: string,
        value?: string,
        description?: string,
    ): Promise<void> {
        const data = await this.getVariableData(key);
        expect(data).not.toBeNull();

        if (value !== undefined) {
            expect(data?.value).toBe(value);
        }
        if (description !== undefined) {
            expect(data?.description).toBe(description);
        }
    }

    async verifyVariableNotExists(key: string): Promise<void> {
        const isVisible = await this.isVariableVisible(key);
        expect(isVisible).toBe(false);
    }
}
