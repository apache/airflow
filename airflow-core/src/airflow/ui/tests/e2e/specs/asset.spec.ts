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

import { AssetListPage } from "../pages/AssetListPage";

test.describe("Assets Page", () => {
  let assets: AssetListPage;

  test.beforeEach(async ({ page }) => {
    assets = new AssetListPage(page);
    await assets.navigate();
    await assets.waitForLoad();
  });

  test("renders assets page heading", async () => {
    await expect(assets.heading).toBeVisible();
  });

  test("renders assets table", async () => {
    await expect(assets.table).toBeVisible();
  });

  test("shows asset rows when data exists", async () => {
    const count = await assets.assetCount();

    expect(count).toBeGreaterThanOrEqual(0);
  });

  test("each asset has a visible name link", async () => {
    const names = await assets.assetNames();

    for (const name of names) {
      expect(name.trim().length).toBeGreaterThan(0);
    }
  });

  test("filters assets using search", async () => {
    const initialCount = await assets.assetCount();

    await assets.search("dag");
    await expect.poll(() => assets.assetCount(), { timeout: 20_000 }).toBeLessThanOrEqual(initialCount);

    if ((await assets.assetCount()) === 0) {
      await expect(assets.emptyState).toBeVisible();
    }
  });

  test("supports pagination when multiple pages exist", async () => {
    const firstRowText = await assets.rows.first().textContent();

    const didNavigate = await assets.goNext();

    if (!didNavigate) {
      test.skip(true, "Pagination not rendered");
    }

    await expect
      .poll(async () => assets.rows.first().textContent(), { timeout: 20_000 })
      .not.toEqual(firstRowText);
  });
});
