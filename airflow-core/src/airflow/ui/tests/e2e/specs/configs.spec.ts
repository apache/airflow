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
import { testConfig } from "playwright.config";

import { ConfigsPage } from "../pages/ConfigsPage";

const escapeForRegex = (value: string): string => value.replaceAll(/[$()*+.?[\\\]^{|}]/g, "\\$&");

test.describe("Configuration Page", () => {
  let configsPage: ConfigsPage;
  const { configPage } = testConfig;

  test.beforeEach(async ({ page }) => {
    configsPage = new ConfigsPage(page);
    await configsPage.navigate(configPage.path);
    await configsPage.waitForLoad();
  });

  test("verify configuration displays", async () => {
    await expect(configsPage.heading).toHaveText(new RegExp(configPage.expectedHeading, "i"));

    if (!configPage.expectsTableData) {
      await expect(configsPage.forbiddenStatus).toBeVisible();
      await expect(
        configsPage.page.getByText(new RegExp(escapeForRegex(configPage.forbiddenMessage), "i")),
      ).toBeVisible();

      return;
    }

    await expect(configsPage.table).toBeVisible();

    const rowCount = await configsPage.getRowCount();

    expect(rowCount).toBeGreaterThan(0);

    const columns = await configsPage.getColumnNames();

    expect(columns).toEqual(expect.arrayContaining(["Section", "Key", "Value"]));
  });

  test("verify configuration page is accessible via Admin menu", async ({ page }) => {
    await page.goto("/");

    await page.getByRole("button", { name: /^admin$/i }).click();

    const configMenuItem = page.getByRole("menuitem", { name: /^config$/i });

    await expect(configMenuItem).toBeVisible();
    await configMenuItem.click();

    await configsPage.waitForLoad();
    expect(page.url()).toContain(configPage.path);

    if (!configPage.expectsTableData) {
      await expect(configsPage.forbiddenStatus).toBeVisible();
      await expect(
        configsPage.page.getByText(new RegExp(escapeForRegex(configPage.forbiddenMessage), "i")),
      ).toBeVisible();

      return;
    }

    await expect(configsPage.table).toBeVisible();
  });

  test("verify configuration section and key are rendered", async () => {
    test.skip(
      !configPage.expectsTableData,
      "Set TEST_CONFIG_PAGE_EXPECTS_TABLE_DATA=true when configuration values are exposed.",
    );

    const sectionAndKeyExists = await configsPage.hasSectionAndKey(
      configPage.expectedSection,
      configPage.expectedKey,
    );

    expect(sectionAndKeyExists).toBe(true);
  });

  test("verify section, key and value are populated in configuration rows", async () => {
    test.skip(
      !configPage.expectsTableData,
      "Set TEST_CONFIG_PAGE_EXPECTS_TABLE_DATA=true when configuration values are exposed.",
    );

    const rowCount = await configsPage.getRowCount();
    const rowsToCheck = Math.min(rowCount, 3);

    for (let i = 0; i < rowsToCheck; i++) {
      const { key, section, value } = await configsPage.getRowDetails(i);

      expect(section).not.toEqual("");
      expect(key).not.toEqual("");
      expect(value).not.toEqual("");
    }
  });
});
