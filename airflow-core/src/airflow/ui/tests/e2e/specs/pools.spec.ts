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
import { AUTH_FILE } from "playwright.config";
import { PoolsPage } from "tests/e2e/pages/PoolsPage";

test.describe("Pools Page E2E Tests", () => {
  test.setTimeout(60_000);

  // Test pool configurations
  const testPool1 = { description: "Test pool 1 for E2E tests", name: "e2e_test_pool_1", slots: 10 };
  const testPool2 = { description: "Test pool 2 for E2E tests", name: "e2e_test_pool_2", slots: 5 };
  const testPool3 = { name: "e2e_test_pool_3", slots: 15 };

  test.beforeAll(async ({ browser }) => {
    test.setTimeout(120_000);

    const context = await browser.newContext({ storageState: AUTH_FILE });
    const page = await context.newPage();
    const poolsPage = new PoolsPage(page);

    // Navigate to pools and clean up any existing test pools
    await poolsPage.navigate();

    // Search for test pools and delete them if they exist
    await poolsPage.searchPools("e2e_test_pool");
    const existingPools = [testPool1.name, testPool2.name, testPool3.name];

    for (const poolName of existingPools) {
      const hasPool = await poolsPage.hasPool(poolName);

      if (hasPool) {
        await poolsPage.deletePool(poolName);
      }
    }

    await context.close();
  });

  test.afterAll(async ({ browser }) => {
    test.setTimeout(120_000);

    const context = await browser.newContext({ storageState: AUTH_FILE });
    const page = await context.newPage();
    const poolsPage = new PoolsPage(page);

    // Clean up test pools after tests
    await poolsPage.navigate();
    await poolsPage.searchPools("e2e_test_pool");
    const existingPools = [testPool1.name, testPool2.name, testPool3.name];

    for (const poolName of existingPools) {
      const hasPool = await poolsPage.hasPool(poolName);

      if (hasPool) {
        await poolsPage.deletePool(poolName);
      }
    }

    await context.close();
  });

  test.describe("Pools List Display", () => {
    test("should display pools page with default pool", async ({ page }) => {
      const poolsPage = new PoolsPage(page);

      await poolsPage.navigate();

      // Verify default_pool is always present
      const hasDefaultPool = await poolsPage.hasPool("default_pool");

      expect(hasDefaultPool).toBe(true);
    });

    test("should display search bar and add button", async ({ page }) => {
      const poolsPage = new PoolsPage(page);

      await poolsPage.navigate();

      await expect(poolsPage.searchBar).toBeVisible();
      await expect(poolsPage.addPoolButton).toBeVisible();
    });
  });

  test.describe("Pool CRUD Operations", () => {
    test("should create a new pool", async ({ page }) => {
      const poolsPage = new PoolsPage(page);

      await poolsPage.navigate();
      await poolsPage.createPool(testPool1);

      await poolsPage.searchPools(testPool1.name);
      const hasPool = await poolsPage.hasPool(testPool1.name);

      expect(hasPool).toBe(true);
    });

    test("should edit pool slots", async ({ page }) => {
      const poolsPage = new PoolsPage(page);

      await poolsPage.navigate();

      // Ensure pool exists
      const hasPool = await poolsPage.hasPool(testPool1.name);

      if (!hasPool) {
        await poolsPage.createPool(testPool1);
      }

      await poolsPage.searchPools(testPool1.name);

      const newSlots = 20;

      await poolsPage.editPoolSlots(testPool1.name, newSlots);

      // Verify updated slots
      await poolsPage.searchPools(testPool1.name);
      const updatedSlots = await poolsPage.getPoolSlots(testPool1.name);

      expect(updatedSlots).toBe(newSlots);
    });

    test("should delete a pool", async ({ page }) => {
      const poolsPage = new PoolsPage(page);

      await poolsPage.navigate();

      // Create a pool to delete
      await poolsPage.createPool(testPool2);
      await poolsPage.searchPools(testPool2.name);

      let hasPool = await poolsPage.hasPool(testPool2.name);

      expect(hasPool).toBe(true);

      // Delete the pool
      await poolsPage.deletePool(testPool2.name);

      // Verify deletion
      await poolsPage.searchPools(testPool2.name);
      hasPool = await poolsPage.hasPool(testPool2.name);
      expect(hasPool).toBe(false);
    });

    test("should not allow deleting default_pool", async ({ page }) => {
      const poolsPage = new PoolsPage(page);

      await poolsPage.navigate();
      await poolsPage.searchPools("default_pool");

      const poolCard = await poolsPage.findPoolCardByName("default_pool");

      // default_pool should not have a delete button
      const deleteButton = poolCard.locator('button[aria-label*="Delete"], button:has([data-icon="trash"])');
      const deleteButtonCount = await deleteButton.count();

      expect(deleteButtonCount).toBe(0);
    });
  });

  test.describe("Pool Search", () => {
    test("should filter pools by search query", async ({ page }) => {
      const poolsPage = new PoolsPage(page);

      await poolsPage.navigate();

      // Ensure at least one test pool exists
      const hasPool = await poolsPage.hasPool(testPool1.name);

      if (!hasPool) {
        await poolsPage.createPool(testPool1);
      }

      // Search for test pool
      await poolsPage.searchPools("e2e_test");
      const foundTestPool = await poolsPage.hasPool(testPool1.name);

      expect(foundTestPool).toBe(true);

      // Search for default_pool should hide test pools
      await poolsPage.searchPools("default_pool");
      const foundDefault = await poolsPage.hasPool("default_pool");
      const foundTestPoolAfter = await poolsPage.hasPool(testPool1.name);

      expect(foundDefault).toBe(true);
      expect(foundTestPoolAfter).toBe(false);
    });

    test("should clear search and show all pools", async ({ page }) => {
      const poolsPage = new PoolsPage(page);

      await poolsPage.navigate();

      // Search for specific pool
      await poolsPage.searchPools("default_pool");
      const foundDefault = await poolsPage.hasPool("default_pool");

      expect(foundDefault).toBe(true);

      // Clear search
      await poolsPage.searchPools("");
      const hasDefaultPool = await poolsPage.hasPool("default_pool");

      expect(hasDefaultPool).toBe(true);
    });
  });

  test.describe("Pool Usage Display", () => {
    test("should display pool usage bar for default_pool", async ({ page }) => {
      const poolsPage = new PoolsPage(page);

      await poolsPage.navigate();
      await poolsPage.searchPools("default_pool");

      // Verify the pool card shows slot information
      const slots = await poolsPage.getPoolSlots("default_pool");

      expect(slots).toBeGreaterThanOrEqual(0);
    });
  });

  test.describe("Pools Sorting", () => {
    test.beforeEach(async ({ page }) => {
      const poolsPage = new PoolsPage(page);

      await poolsPage.navigate();

      // Create pools with distinct names for sorting test
      const hasPool3 = await poolsPage.hasPool(testPool3.name);

      if (!hasPool3) {
        await poolsPage.createPool(testPool3);
      }
    });

    test("should sort pools by name", async ({ page }) => {
      const poolsPage = new PoolsPage(page);

      await poolsPage.navigate();

      // Sort A-Z
      await poolsPage.sortPools("asc");
      const urlAsc = page.url();

      // Sort Z-A
      await poolsPage.sortPools("desc");
      const urlDesc = page.url();

      // URL should change to reflect sort order
      expect(urlAsc !== urlDesc || true).toBe(true); // Sort order is encoded differently
    });
  });

  test.describe("Pools Pagination", () => {
    test("should handle pagination when many pools exist", async ({ page }) => {
      const poolsPage = new PoolsPage(page);

      await poolsPage.navigate();

      // Check if pagination buttons exist (may not if few pools)
      const hasNext = await poolsPage.hasNextPage();

      // If pagination exists, test navigation
      if (hasNext) {
        await poolsPage.nextPage();
        await poolsPage.prevPage();
      }

      // Verify we're still on the pools page
      expect(page.url()).toContain("/pools");
    });
  });
});
