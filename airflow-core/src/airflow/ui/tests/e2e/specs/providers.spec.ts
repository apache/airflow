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
    await expect(providers.rows.first()).toBeVisible();
  });

  test("Verify the providers list displays", async () => {
    await expect(providers.table).toBeVisible();
  });

  test("Verify package name, version, and description are not blank", async () => {
    await expect(providers.rows.first()).toBeVisible();

    for (let i = 0; i < 2; i++) {
      const row = providers.rows.nth(i);
      const cells = row.locator("td");

      await expect(cells.nth(0).locator("a")).not.toBeEmpty();
      await expect(cells.nth(1)).not.toBeEmpty();
      await expect(cells.nth(2)).not.toBeEmpty();
    }
  });
});
