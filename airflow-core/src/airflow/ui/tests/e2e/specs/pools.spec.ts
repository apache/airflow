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
import { test } from "@playwright/test";
import { AUTH_FILE } from "playwright.config";
import { PoolsPage } from "tests/e2e/pages/PoolsPage";

test.describe("Pools Page", () => {
  test.setTimeout(60_000);

  let poolsPage: PoolsPage;
  const testPoolName = `test_pool_${Date.now()}`;
  const testPoolSlots = 10;
  const testPoolDescription = "E2E test pool";
  const updatedSlots = 25;
  const createDeletePoolName = `test_crud_pool_${Date.now()}`;
  const createDeletePoolSlots = 5;

  test.beforeAll(async ({ browser }) => {
    const context = await browser.newContext({ storageState: AUTH_FILE });
    const page = await context.newPage();
    const setupPoolsPage = new PoolsPage(page);

    await setupPoolsPage.navigate();
    await setupPoolsPage.createPool(testPoolName, testPoolSlots, testPoolDescription);

    await context.close();
  });

  test.afterAll(async ({ browser }) => {
    const context = await browser.newContext({ storageState: AUTH_FILE });
    const page = await context.newPage();

    await page.request.delete(`/api/v2/pools/${encodeURIComponent(testPoolName)}`);
    await page.request.delete(`/api/v2/pools/${encodeURIComponent(createDeletePoolName)}`);

    await context.close();
  });

  test.beforeEach(({ page }) => {
    poolsPage = new PoolsPage(page);
  });

  test("verify pools list displays with default_pool", async () => {
    await poolsPage.navigate();
    await poolsPage.verifyPoolsListDisplays();
  });

  test("verify test pool exists in list", async () => {
    await poolsPage.navigate();
    await poolsPage.verifyPoolExists(testPoolName);
  });

  test("verify pool slots display correctly", async () => {
    await poolsPage.navigate();
    await poolsPage.verifyPoolSlots(testPoolName, testPoolSlots);
  });

  test("edit pool slots", async () => {
    await poolsPage.navigate();
    await poolsPage.editPoolSlots(testPoolName, updatedSlots);
    await poolsPage.navigate();
    await poolsPage.verifyPoolSlots(testPoolName, updatedSlots);
  });

  test("verify pool usage displays", async () => {
    await poolsPage.navigate();
    await poolsPage.verifyPoolUsageDisplays("default_pool");
  });

  test("create a new pool", async () => {
    await poolsPage.navigate();
    await poolsPage.createPool(createDeletePoolName, createDeletePoolSlots, "Created pool");
    await poolsPage.navigate();
    await poolsPage.verifyPoolExists(createDeletePoolName);
  });

  test("delete pool", async () => {
    await poolsPage.navigate();
    await poolsPage.verifyPoolExists(createDeletePoolName);
    await poolsPage.deletePool(createDeletePoolName);
    await poolsPage.verifyPoolNotExists(createDeletePoolName);
  });
});
