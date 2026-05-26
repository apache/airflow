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
import { expect, test } from "tests/e2e/fixtures";

test.describe("Providers Page", () => {
  test.beforeEach(async ({ providersPage }) => {
    await providersPage.navigate();
  });

  test("verify providers page heading", async ({ providersPage }) => {
    await expect(providersPage.heading).toBeVisible();
  });

  test("Verify Providers page is accessible via Admin menu", async ({ page, providersPage }) => {
    await page.goto("/");

    await page.getByRole("button", { name: /^admin$/i }).click();

    const providersItem = page.getByRole("menuitem", { name: /^providers$/i });

    await expect(providersItem).toBeVisible();
    await providersItem.click();

    await providersPage.waitForLoad();
    await expect(providersPage.heading).toBeVisible();
    expect(await providersPage.getRowCount()).toBeGreaterThan(0);
  });

  test("Verify the providers list displays", async ({ providersPage }) => {
    await expect(providersPage.table).toBeVisible();
  });

  test("Verify package name, version, and description are not blank", async ({ providersPage }) => {
    const count = await providersPage.getRowCount();

    expect(count).toBeGreaterThan(0);

    for (let i = 0; i < 2; i++) {
      const { description, packageName, version } = await providersPage.getRowDetails(i);

      expect(packageName).not.toEqual("");
      expect(version).not.toEqual("");
      expect(description).not.toEqual("");
    }
  });
});
