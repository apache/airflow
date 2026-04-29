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
import { testConfig } from "playwright.config";
import { expect } from "tests/e2e/fixtures";
import { test } from "tests/e2e/fixtures/asset-data";

test.describe("Assets Page", () => {
  // assetData is triggered once per worker via beforeEach.
  test.beforeEach(async ({ assetData: _data, assetListPage }) => {
    await assetListPage.navigate();
    await assetListPage.waitForLoad();
  });

  test("verify assets page heading", async ({ assetListPage }) => {
    await expect(assetListPage.heading).toBeVisible();
  });

  test("verify assets table", async ({ assetListPage }) => {
    await expect(assetListPage.table).toBeVisible();
  });

  test("verify asset rows when data exists", async ({ assetListPage }) => {
    await expect(assetListPage.rows.first()).toBeVisible();
  });

  test("verify asset has a visible name link", async ({ assetListPage }) => {
    await expect(assetListPage.rows.locator("td a").first()).toBeVisible();
  });

  test("verify clicking an asset navigates to detail page", async ({ assetListPage, page }) => {
    const name = await assetListPage.openFirstAsset();

    await expect(page).toHaveURL(/\/assets\/.+/);
    await expect(page.getByRole("heading", { name: new RegExp(name, "i") })).toBeVisible();
  });

  test("verify assets using search", async ({ assetListPage }) => {
    await expect(assetListPage.rows.first()).toBeVisible();

    const searchTerm = testConfig.asset.name;

    await assetListPage.searchInput.fill(searchTerm);

    await expect
      .poll(
        async () => {
          const links = await assetListPage.rows.locator("td a").allTextContents();

          return (
            links.length > 0 && links.every((name) => name.toLowerCase().includes(searchTerm.toLowerCase()))
          );
        },
        { intervals: [500], timeout: 30_000 },
      )
      .toBe(true);
  });

  test("verify asset details and dependencies", async ({ assetDetailPage }) => {
    const assetName = testConfig.asset.name;

    await assetDetailPage.goto();
    await assetDetailPage.clickOnAsset(assetName);
    await expect(assetDetailPage.getHeading(assetName)).toBeVisible();
    await assetDetailPage.verifyProducingTasks();
    await assetDetailPage.verifyScheduledDags();
  });
});
