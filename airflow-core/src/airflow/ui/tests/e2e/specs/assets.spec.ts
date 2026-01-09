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
import { AssetsPage } from "tests/e2e/pages/AssetsPage";

test.describe("Assets Page", () => {
  let assetsPage: AssetsPage;
  const assetName = "s3://dag1/output_1.txt";

  test.beforeEach(({ page }) => {
    assetsPage = new AssetsPage(page);
  });

  test("should verify asset details and dependencies", async () => {
    await assetsPage.goto();

    await assetsPage.clickOnAsset(assetName);

    await assetsPage.verifyAssetDetails(assetName);

    await assetsPage.verifyProducingTasks(1);

    await assetsPage.verifyScheduledDags(1);
  });
});
