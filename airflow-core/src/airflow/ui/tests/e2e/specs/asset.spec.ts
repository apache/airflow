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
import { AUTH_FILE, testConfig } from "playwright.config";

import { AssetDetailPage } from "../pages/AssetDetailPage";
import { AssetListPage } from "../pages/AssetListPage";
import { DagsPage } from "../pages/DagsPage";

test.describe("Assets Page", () => {
  let assets: AssetListPage;

  test.beforeAll(async ({ browser }) => {
    test.setTimeout(3 * 60 * 1000);
    const context = await browser.newContext({ storageState: AUTH_FILE });
    const page = await context.newPage();
    const dagsPage = new DagsPage(page);

    await dagsPage.triggerDag("asset_produces_1");
    await expect
      .poll(
        async () => {
          const response = await page.request.get(
            `/api/v2/dags/asset_produces_1/dagRuns?order_by=-start_date&limit=1`,
          );
          const data = (await response.json()) as { dag_runs: Array<{ state: string }> };

          return data.dag_runs[0]?.state ?? "pending";
        },
        { intervals: [2000], timeout: 120_000 },
      )
      .toBe("success");
    await context.close();
  });

  test.beforeEach(async ({ page }) => {
    assets = new AssetListPage(page);
    await assets.navigate();
    await assets.waitForLoad();
  });

  test("verify assets page heading", async () => {
    await expect(assets.heading).toBeVisible();
  });

  test("verify assets table", async () => {
    await expect(assets.table).toBeVisible();
  });

  test("verify asset rows when data exists", async () => {
    const count = await assets.assetCount();

    expect(count).toBeGreaterThanOrEqual(0);
  });

  test("verify asset has a visible name link", async () => {
    const names = await assets.assetNames();

    for (const name of names) {
      expect(name.trim().length).toBeGreaterThan(0);
    }
  });

  test("verify clicking an asset navigates to detail page", async ({ page }) => {
    const name = await assets.openFirstAsset();

    await expect(page).toHaveURL(/\/assets\/.+/);
    await expect(page.getByRole("heading", { name: new RegExp(name, "i") })).toBeVisible();
  });

  test("verify assets using search", async () => {
    const initialCount = await assets.assetCount();

    expect(initialCount).toBeGreaterThan(0);

    const searchTerm = testConfig.asset.name;

    await assets.searchInput.fill(searchTerm);

    // Wait for filtered results - count should decrease OR stay same if search matches all
    await expect
      .poll(
        async () => {
          const links = await assets.rows.locator("td a").allTextContents();

          // Return true when we have results that match the search
          return (
            links.length > 0 && links.every((name) => name.toLowerCase().includes(searchTerm.toLowerCase()))
          );
        },
        { intervals: [500], timeout: 30_000 },
      )
      .toBe(true);

    const names = await assets.assetNames();

    expect(names.length).toBeGreaterThan(0);

    for (const name of names) {
      expect(name.toLowerCase()).toContain(searchTerm.toLowerCase());
    }
  });

  test("verify asset details and dependencies", async ({ page }) => {
    const assetDetailPage = new AssetDetailPage(page);
    const assetName = testConfig.asset.name;

    await assetDetailPage.goto();

    await assetDetailPage.clickOnAsset(assetName);

    await assetDetailPage.verifyAssetDetails(assetName);

    await assetDetailPage.verifyProducingTasks(1);

    await assetDetailPage.verifyScheduledDags(1);
  });
});
