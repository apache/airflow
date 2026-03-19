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

import { ConfigurationPage } from "../pages/ConfigurationPage";

test.describe("Configuration Page", () => {
  let configPage: ConfigurationPage;

  test.beforeEach(async ({ page }) => {
    configPage = new ConfigurationPage(page);
    await configPage.navigate();
    await configPage.waitForLoad();
  });

  test("verify configuration displays", async () => {
    await expect(configPage.heading).toBeVisible();
    await expect(configPage.table).toBeVisible();

    await expect(configPage.rows).not.toHaveCount(0);

    const firstRow = configPage.rows.nth(0);

    await expect(firstRow.locator("td").nth(0)).not.toBeEmpty();
    await expect(firstRow.locator("td").nth(1)).not.toBeEmpty();
    await expect(firstRow.locator("td").nth(2)).not.toBeEmpty();
  });
});
