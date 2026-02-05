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

  test("verify providers page heading", async () => {
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

  test("verify providers pagination", async () => {
    const limit = 5;

    await providers.navigateTo(`/providers?offset=0&limit=${limit}`);
    await providers.waitForLoad();

    const rows = await providers.getRowCount();

    expect(rows).toBeGreaterThan(0);

    const initialProviderNames = await providers.providerNames();

    expect(initialProviderNames.length).toBeGreaterThan(0);

    await expect(providers.paginationNextButton).toBeVisible();
    await expect(providers.paginationPrevButton).toBeVisible();

    await providers.paginationNextButton.click();
    await providers.waitForLoad();

    await providers.page.waitForURL((url) => {
      const u = new URL(url);
      const offset = u.searchParams.get("offset");

      return offset !== null && offset !== "0";
    });

    const rowsPage2 = await providers.getRowCount();

    expect(rowsPage2).toBeGreaterThan(0);

    const ProviderNamesAfterNext = await providers.providerNames();

    expect(ProviderNamesAfterNext.length).toBeGreaterThan(0);
    expect(ProviderNamesAfterNext).not.toEqual(initialProviderNames);

    await providers.paginationPrevButton.click();
    await providers.waitForLoad();

    await providers.page.waitForURL((url) => {
      const u = new URL(url);
      const offset = u.searchParams.get("offset");

      return offset === "0" || offset === null;
    });

    const rowsBack = await providers.getRowCount();

    expect(rowsBack).toBeGreaterThan(0);
  });
});
