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

import { AssetDetailPage } from "../pages/AssetDetailPage";
import { DagsPage } from "../pages/DagsPage";

test.describe("Asset Details Page", () => {
  let assetDetailPage: AssetDetailPage;
  const assetName = "s3://dag1/output_1.txt";

  test.beforeAll(async ({ browser }) => {
    test.setTimeout(3 * 60 * 1000);
    const context = await browser.newContext({ storageState: AUTH_FILE });
    const page = await context.newPage();
    const dagsPage = new DagsPage(page);

    // Trigger asset_produces_1 to create asset data
    await dagsPage.triggerDag("asset_produces_1");
    await expect
      .poll(
        async () => {
          try {
            const response = await page.request.get(
              `/api/v2/dags/asset_produces_1/dagRuns?order_by=-start_date&limit=1`,
              { timeout: 30_000 },
            );
            const data = (await response.json()) as { dag_runs: Array<{ state: string }> };

            return data.dag_runs[0]?.state ?? "pending";
          } catch {
            return "pending";
          }
        },
        { intervals: [2000], timeout: 120_000 },
      )
      .toBe("success");
    await context.close();
  });

  test.beforeEach(({ page }) => {
    assetDetailPage = new AssetDetailPage(page);
  });

  test("verify asset details and dependencies", async () => {
    await assetDetailPage.goto();

    await assetDetailPage.clickOnAsset(assetName);

    await assetDetailPage.verifyAssetDetails(assetName);

    await assetDetailPage.verifyProducingTasks(1);

    await assetDetailPage.verifyScheduledDags(1);
  });
});
