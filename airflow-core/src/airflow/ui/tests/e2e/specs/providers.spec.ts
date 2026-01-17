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

import { ProvidersPage } from "../pages/ProvidersPage";

test.describe("Providers Page", () => {
  let providers: ProvidersPage;

  test.beforeEach(async ({ page }) => {
    providers = new ProvidersPage(page);
    await providers.navigate();
    await providers.waitForLoad();
  });

  test("verify assets page heading", async () => {
    await expect(providers.heading).toBeVisible();
  });

  test("Verify Providers page is accessible via Admin menu", async ({ page }) => {
    await page.goto("/");

    await page.getByRole("button", { name: /^admin$/i }).click();

    // Click Providers
    const providersItem = page.getByRole("menuitem", { name: /^providers$/i });

    await expect(providersItem).toBeVisible();
    await providersItem.click();

    await providers.waitForLoad();
    // Assert Providers page loaded
    await expect(providers.heading).toBeVisible();
    expect(await providers.getRowCount()).toBeGreaterThan(0);
  });

  test("Verify the providers list displays", async () => {
    await expect(providers.table).toBeVisible();
  });

  test("Verify package name, version, and description are not blank", async () => {
    const count = await providers.getRowCount();

    expect(count).toBeGreaterThan(0);

    for (let i = 0; i < 2; i++) {
      const { description, packageName, version } = await providers.getRowDetails(i);

      expect(packageName).not.toEqual("");
      expect(version).not.toEqual("");
      expect(description).not.toEqual("");
    }
  });

  test("verify pagination controls navigate between pages", async () => {
    await providers.navigateTo("/providers?limit=5&offset=0");
    await providers.waitForLoad();

    const page1Initial = await providers.providerNames();

    expect(page1Initial.length).toBeGreaterThan(0);

    const pagination = providers.page.locator('[data-scope="pagination"]');

    await pagination.getByRole("button", { name: /^page 2$/i }).click();
    await expect.poll(() => providers.providerNames(), { timeout: 30_000 }).not.toEqual(page1Initial);

    const page2Assets = await providers.providerNames();

    await pagination.getByRole("button", { name: /page 1/i }).click();

    await expect.poll(() => providers.providerNames(), { timeout: 30_000 }).not.toEqual(page2Assets);
  });
});
