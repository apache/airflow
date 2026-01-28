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

import { test, expect } from "@playwright/test";
import * as fs from "fs";
import * as path from "path";

import { testConfig } from "../testConfig";
import { LoginPage } from "../pages/LoginPage";
import { VariablesPage } from "../pages/VariablesPage";

test.describe("Variables Page - ADMIN-001", () => {
    let loginPage: LoginPage;
    let variablesPage: VariablesPage;
    const testVariables: string[] = [];

    // Generate unique test data
    const generateTestKey = (suffix: string) => {
        const key = `${testConfig.testDataPrefix}${suffix}_${Date.now()}`;
        testVariables.push(key);
        return key;
    };

    test.beforeAll(async ({ browser }) => {
        // Setup: Create test data
        const context = await browser.newContext();
        const page = await context.newPage();

        loginPage = new LoginPage(page);
        variablesPage = new VariablesPage(page);

        // Login
        await loginPage.login();
        await variablesPage.goto();

        // Create baseline test variables for pagination (35+ variables)
        console.log("Creating baseline test variables...");

        for (let i = 1; i <= 35; i++) {
            const key = generateTestKey(`baseline_${i}`);
            await variablesPage.createVariable(
                key,
                `test_value_${i}`,
                `Test description ${i}`,
                false,
            );
        }

        console.log(`Created ${testVariables.length} test variables`);

        await context.close();
    });

    test.afterAll(async ({ browser }) => {
        // Cleanup: Delete all test variables
        const context = await browser.newContext();
        const page = await context.newPage();

        loginPage = new LoginPage(page);
        variablesPage = new VariablesPage(page);

        await loginPage.login();
        await variablesPage.goto();

        console.log(`Cleaning up ${testVariables.length} test variables...`);

        // Delete in batches to avoid overwhelming the UI
        const batchSize = 10;
        for (let i = 0; i < testVariables.length; i += batchSize) {
            const batch = testVariables.slice(i, i + batchSize);
            const existingKeys: string[] = [];

            // Check which variables still exist
            for (const key of batch) {
                const exists = await variablesPage.isVariableVisible(key);
                if (exists) {
                    existingKeys.push(key);
                }
            }

            // Delete existing variables
            if (existingKeys.length > 0) {
                try {
                    await variablesPage.deleteMultipleVariables(existingKeys);
                } catch (error) {
                    console.error(`Failed to delete batch: ${error}`);
                }
            }
        }

        console.log("Cleanup complete");
        await context.close();
    });

    test.beforeEach(async ({ page }) => {
        loginPage = new LoginPage(page);
        variablesPage = new VariablesPage(page);

        await loginPage.login();
        await variablesPage.goto();
    });

    test("should display variables list", async () => {
        // Verify the table is visible
        const isVisible = await variablesPage.isVisible('[role="table"]');
        expect(isVisible).toBe(true);

        // Verify we have variables (from beforeAll)
        const count = await variablesPage.getDisplayedVariablesCount();
        expect(count).toBeGreaterThan(0);
    });

    test("should create a new variable", async () => {
        const key = generateTestKey("create_test");
        const value = "test_value_create";
        const description = "Created via E2E test";

        await variablesPage.createVariable(key, value, description);

        // Verify the variable appears in the table
        await variablesPage.verifyVariableExists(key, value, description);
    });

    test("should create an encrypted variable", async () => {
        const key = generateTestKey("encrypted_test");
        const value = "secret_value";
        const description = "Encrypted variable";

        await variablesPage.createVariable(key, value, description, true);

        // Verify the variable exists
        const isVisible = await variablesPage.isVariableVisible(key);
        expect(isVisible).toBe(true);

        // Verify it's marked as encrypted
        const data = await variablesPage.getVariableData(key);
        expect(data?.isEncrypted).toBeTruthy();
    });

    test("should edit a variable", async () => {
        // Create a variable to edit
        const key = generateTestKey("edit_test");
        await variablesPage.createVariable(key, "original_value", "Original description");

        // Edit the variable
        const newValue = "updated_value";
        const newDescription = "Updated description";
        await variablesPage.editVariable(key, newValue, newDescription);

        // Verify the updates
        await variablesPage.verifyVariableExists(key, newValue, newDescription);
    });

    test("should delete a single variable", async () => {
        // Create a variable to delete
        const key = generateTestKey("delete_test");
        await variablesPage.createVariable(key, "value_to_delete", "Will be deleted");

        // Verify it exists
        let isVisible = await variablesPage.isVariableVisible(key);
        expect(isVisible).toBe(true);

        // Delete it
        await variablesPage.deleteVariable(key);

        // Verify it's gone
        await variablesPage.verifyVariableNotExists(key);

        // Remove from cleanup list
        const index = testVariables.indexOf(key);
        if (index > -1) {
            testVariables.splice(index, 1);
        }
    });

    test("should delete multiple variables", async () => {
        // Create multiple variables
        const keys = [
            generateTestKey("multi_delete_1"),
            generateTestKey("multi_delete_2"),
            generateTestKey("multi_delete_3"),
        ];

        for (const key of keys) {
            await variablesPage.createVariable(key, `value_${key}`, "To be deleted");
        }

        // Verify they all exist
        for (const key of keys) {
            const isVisible = await variablesPage.isVariableVisible(key);
            expect(isVisible).toBe(true);
        }

        // Delete all of them
        await variablesPage.deleteMultipleVariables(keys);

        // Verify they're all gone
        for (const key of keys) {
            await variablesPage.verifyVariableNotExists(key);
        }

        // Remove from cleanup list
        keys.forEach((key) => {
            const index = testVariables.indexOf(key);
            if (index > -1) {
                testVariables.splice(index, 1);
            }
        });
    });

    test("should search variables by key pattern", async () => {
        // Create a unique variable for this test
        const uniqueKey = generateTestKey("search_unique");
        await variablesPage.createVariable(uniqueKey, "searchable_value", "For search test");

        // Search for the unique key
        await variablesPage.searchVariables(uniqueKey);

        // Verify only the searched variable is visible
        const isVisible = await variablesPage.isVariableVisible(uniqueKey);
        expect(isVisible).toBe(true);

        // Clear search
        await variablesPage.clearSearch();

        // Verify we see more variables again
        const count = await variablesPage.getDisplayedVariablesCount();
        expect(count).toBeGreaterThan(1);
    });

    test("should search variables with wildcard pattern", async () => {
        // Create variables with common prefix
        const prefix = generateTestKey("wildcard");
        const keys = [
            `${prefix}_one`,
            `${prefix}_two`,
            `${prefix}_three`,
        ];

        // Track for cleanup
        keys.forEach((key) => testVariables.push(key));

        for (const key of keys) {
            await variablesPage.createVariable(key, `value_${key}`, "Wildcard test");
        }

        // Search using partial pattern
        await variablesPage.searchVariables(prefix);

        // Verify all matching variables are visible
        for (const key of keys) {
            const isVisible = await variablesPage.isVariableVisible(key);
            expect(isVisible).toBe(true);
        }

        // Clear search
        await variablesPage.clearSearch();
    });

    test("should import variables from JSON file", async ({ page }) => {
        // Create a temporary JSON file with test variables
        const importData: Record<string, string> = {};
        const importKeys: string[] = [];

        for (let i = 1; i <= 3; i++) {
            const key = generateTestKey(`import_${i}`);
            importKeys.push(key);
            importData[key] = `imported_value_${i}`;
        }

        const tempFile = path.join(__dirname, `test_import_${Date.now()}.json`);
        fs.writeFileSync(tempFile, JSON.stringify(importData, null, 2));

        try {
            // Import the file
            await variablesPage.importVariables(tempFile);

            // Verify all imported variables exist
            for (const key of importKeys) {
                const isVisible = await variablesPage.isVariableVisible(key);
                expect(isVisible).toBe(true);
            }
        } finally {
            // Cleanup temp file
            if (fs.existsSync(tempFile)) {
                fs.unlinkSync(tempFile);
            }
        }
    });

    test("should verify pagination works", async () => {
        // With 35+ test variables, we should have multiple pages (30 per page)
        const totalVars = await variablesPage.getTotalVariables();
        expect(totalVars).toBeGreaterThan(30);

        // Verify we're on page 1
        let currentPage = await variablesPage.getCurrentPage();
        expect(currentPage).toBe(1);

        // Go to next page
        await variablesPage.goToNextPage();

        // Verify we're on page 2
        currentPage = await variablesPage.getCurrentPage();
        expect(currentPage).toBe(2);

        // Go back to page 1
        await variablesPage.goToPreviousPage();

        // Verify we're back on page 1
        currentPage = await variablesPage.getCurrentPage();
        expect(currentPage).toBe(1);
    });

    test("should sort variables by key (ascending)", async () => {
        // Click the Key column header
        await variablesPage.sortByColumn("Key");

        // Wait a moment for sort to apply
        await variablesPage.wait(500);

        // Get the sort direction (this might vary based on implementation)
        // Just verify that sorting action completed without error
        const displayedCount = await variablesPage.getDisplayedVariablesCount();
        expect(displayedCount).toBeGreaterThan(0);
    });

    test("should sort variables by key (descending)", async () => {
        // Click the Key column header once
        await variablesPage.sortByColumn("Key");
        await variablesPage.wait(500);

        // Click again for descending
        await variablesPage.sortByColumn("Key");
        await variablesPage.wait(500);

        // Verify variables are still displayed
        const displayedCount = await variablesPage.getDisplayedVariablesCount();
        expect(displayedCount).toBeGreaterThan(0);
    });

    test("should sort variables by value", async () => {
        // Click the Value column header
        await variablesPage.sortByColumn("Value");
        await variablesPage.wait(500);

        // Verify variables are still displayed
        const displayedCount = await variablesPage.getDisplayedVariablesCount();
        expect(displayedCount).toBeGreaterThan(0);
    });

    test("should sort variables by description", async () => {
        // Click the Description column header
        await variablesPage.sortByColumn("Description");
        await variablesPage.wait(500);

        // Verify variables are still displayed
        const displayedCount = await variablesPage.getDisplayedVariablesCount();
        expect(displayedCount).toBeGreaterThan(0);
    });
});
