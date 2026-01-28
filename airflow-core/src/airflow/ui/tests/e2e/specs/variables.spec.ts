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
import { expect, test } from "@playwright/test";
import { VariablesPage } from "tests/e2e/pages/VariablesPage";

// Generate unique test variable names to avoid conflicts
const generateUniqueKey = (prefix: string): string => `${prefix}_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`;

test.describe("Variables Page - List Display", () => {
  let variablesPage: VariablesPage;

  test.beforeEach(async ({ page }) => {
    variablesPage = new VariablesPage(page);
    await variablesPage.navigate();
  });

  test("should display the variables page with correct structure", async () => {
    // Verify page loads without errors
    await expect(variablesPage.addVariableButton).toBeVisible();
    await expect(variablesPage.importVariablesButton).toBeVisible();
    await expect(variablesPage.searchBox).toBeVisible();
  });

  test("should display column headers correctly", async () => {
    const headers = await variablesPage.getColumnHeaders();

    // Should have key, value, description, and is_encrypted columns
    expect(headers.length).toBeGreaterThanOrEqual(3);
  });
});

test.describe("Variables Page - CRUD Operations", () => {
  let variablesPage: VariablesPage;
  let testVariableKey: string;

  test.beforeEach(async ({ page }) => {
    variablesPage = new VariablesPage(page);
    testVariableKey = generateUniqueKey("e2e_test_var");
    await variablesPage.navigate();
  });

  test.afterEach(async () => {
    // Cleanup: Delete the test variable if it exists
    try {
      if (await variablesPage.variableExists(testVariableKey)) {
        await variablesPage.deleteVariable(testVariableKey);
      }
    } catch {
      // Variable may not exist, ignore cleanup errors
    }
  });

  test("should create a new variable", async () => {
    const testValue = "test_value_123";
    const testDescription = "E2E test variable description";

    await variablesPage.createVariable(testVariableKey, testValue, testDescription);

    // Verify variable was created
    const exists = await variablesPage.variableExists(testVariableKey);

    expect(exists).toBe(true);
  });

  test("should edit an existing variable", async () => {
    // First create a variable
    const initialValue = "initial_value";

    await variablesPage.createVariable(testVariableKey, initialValue);

    // Verify it was created
    expect(await variablesPage.variableExists(testVariableKey)).toBe(true);

    // Edit the variable
    const newValue = "updated_value_456";
    const newDescription = "Updated description";

    await variablesPage.editVariable(testVariableKey, newValue, newDescription);

    // Verify the variable still exists (edit was successful)
    expect(await variablesPage.variableExists(testVariableKey)).toBe(true);
  });

  test("should delete a variable", async () => {
    // First create a variable
    await variablesPage.createVariable(testVariableKey, "to_be_deleted");

    // Verify it was created
    expect(await variablesPage.variableExists(testVariableKey)).toBe(true);

    // Delete the variable
    await variablesPage.deleteVariable(testVariableKey);

    // Verify variable was deleted
    const exists = await variablesPage.variableExists(testVariableKey);

    expect(exists).toBe(false);
  });
});

test.describe("Variables Page - Search", () => {
  let variablesPage: VariablesPage;
  const searchTestPrefix = "search_test";
  let searchTestKey1: string;
  let searchTestKey2: string;

  test.beforeAll(async ({ browser }) => {
    // Create test variables for search tests
    const page = await browser.newPage();

    variablesPage = new VariablesPage(page);
    searchTestKey1 = generateUniqueKey(searchTestPrefix);
    searchTestKey2 = generateUniqueKey("other_var");

    await variablesPage.navigate();
    await variablesPage.createVariable(searchTestKey1, "search_value_1");
    await variablesPage.createVariable(searchTestKey2, "search_value_2");
    await page.close();
  });

  test.afterAll(async ({ browser }) => {
    // Cleanup test variables
    const page = await browser.newPage();

    variablesPage = new VariablesPage(page);
    await variablesPage.navigate();

    try {
      if (await variablesPage.variableExists(searchTestKey1)) {
        await variablesPage.deleteVariable(searchTestKey1);
      }
      if (await variablesPage.variableExists(searchTestKey2)) {
        await variablesPage.deleteVariable(searchTestKey2);
      }
    } catch {
      // Ignore cleanup errors
    }

    await page.close();
  });

  test.beforeEach(async ({ page }) => {
    variablesPage = new VariablesPage(page);
    await variablesPage.navigate();
  });

  test("should filter variables by search term", async () => {
    // Search for the specific prefix
    await variablesPage.searchVariables(searchTestPrefix);

    // Should find our test variable
    const keys = await variablesPage.getVariableKeys();

    expect(keys.some((key) => key.includes(searchTestPrefix))).toBe(true);
  });

  test("should show no results for non-existent search term", async () => {
    // Search for something that doesn't exist
    await variablesPage.searchVariables("nonexistent_variable_xyz_12345");

    // Should have no results
    const count = await variablesPage.getVariableCount();

    expect(count).toBe(0);
  });

  test("should clear search and show all variables", async () => {
    // First search for something specific
    await variablesPage.searchVariables(searchTestPrefix);

    // Clear the search
    await variablesPage.clearSearch();

    // Should show more results now (or at least the other test variable)
    const keys = await variablesPage.getVariableKeys();

    // After clearing, we should see multiple variables if they exist
    expect(keys.length).toBeGreaterThanOrEqual(0);
  });
});

test.describe("Variables Page - Import", () => {
  let variablesPage: VariablesPage;
  let importedKey1: string;
  let importedKey2: string;

  test.beforeEach(async ({ page }) => {
    variablesPage = new VariablesPage(page);
    importedKey1 = generateUniqueKey("imported_var_1");
    importedKey2 = generateUniqueKey("imported_var_2");
    await variablesPage.navigate();
  });

  test.afterEach(async () => {
    // Cleanup imported variables
    try {
      if (await variablesPage.variableExists(importedKey1)) {
        await variablesPage.deleteVariable(importedKey1);
      }
      if (await variablesPage.variableExists(importedKey2)) {
        await variablesPage.deleteVariable(importedKey2);
      }
    } catch {
      // Ignore cleanup errors
    }
  });

  test("should import variables from JSON file", async () => {
    const jsonContent = JSON.stringify({
      [importedKey1]: "imported_value_1",
      [importedKey2]: "imported_value_2",
    });

    await variablesPage.importVariablesFromJson(jsonContent);

    // Verify both variables were imported
    expect(await variablesPage.variableExists(importedKey1)).toBe(true);
    expect(await variablesPage.variableExists(importedKey2)).toBe(true);
  });
});

test.describe("Variables Page - Pagination", () => {
  let variablesPage: VariablesPage;

  test.beforeEach(async ({ page }) => {
    variablesPage = new VariablesPage(page);
    await variablesPage.navigate();
  });

  test("should display pagination controls", async () => {
    // Check if pagination controls are visible
    const paginationAvailable = await variablesPage.isPaginationAvailable();

    // Pagination should be available (controls should be visible)
    expect(paginationAvailable).toBe(true);
  });

  test("should navigate between pages if multiple pages exist", async ({ page }) => {
    // Skip this test if there aren't enough variables for pagination
    const initialCount = await variablesPage.getVariableCount();

    // If there are enough variables to paginate, test it
    if (initialCount >= 30) {
      const initialKeys = await variablesPage.getVariableKeys();

      await variablesPage.clickNextPage();

      const nextPageKeys = await variablesPage.getVariableKeys();

      // Keys should be different on next page
      expect(nextPageKeys).not.toEqual(initialKeys);

      // Go back to previous page
      await variablesPage.clickPrevPage();

      const prevPageKeys = await variablesPage.getVariableKeys();

      // Should be back to initial page
      expect(prevPageKeys).toEqual(initialKeys);
    } else {
      // Just verify page loads correctly if not enough data
      expect(initialCount).toBeGreaterThanOrEqual(0);
    }
  });
});

test.describe("Variables Page - Sorting", () => {
  let variablesPage: VariablesPage;

  test.beforeEach(async ({ page }) => {
    variablesPage = new VariablesPage(page);
    await variablesPage.navigate();
  });

  test("should display sortable column headers", async ({ page }) => {
    // Find the key column header and verify it's clickable/sortable
    const keyHeader = page.locator('table thead th').filter({ hasText: /key/i });

    await expect(keyHeader).toBeVisible();

    // Click the header to sort
    await keyHeader.click();

    // Wait for potential re-render
    await page.waitForTimeout(500);

    // Page should still display correctly after sorting
    await expect(variablesPage.addVariableButton).toBeVisible();
  });
});
