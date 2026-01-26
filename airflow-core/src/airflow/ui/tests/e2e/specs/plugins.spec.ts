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

import { PluginsPage } from "../pages/PluginsPage";

test.describe("Plugins Page", () => {
  let pluginsPage: PluginsPage;

  test.beforeEach(async ({ page }) => {
    pluginsPage = new PluginsPage(page);
    await pluginsPage.navigate();
    await pluginsPage.waitForLoad();
  });

  test("verify plugins page heading is visible", async () => {
    await expect(pluginsPage.heading).toBeVisible();
  });

  test("verify plugins table is visible", async () => {
    await expect(pluginsPage.table).toBeVisible();
  });

  test("verify plugins list displays with data", async () => {
    const count = await pluginsPage.getPluginCount();

    expect(count).toBeGreaterThan(0);
  });

  test("verify each plugin has a name", async () => {
    const pluginNames = await pluginsPage.getPluginNames();

    expect(pluginNames.length).toBeGreaterThan(0);

    for (const name of pluginNames) {
      expect(name.trim().length).toBeGreaterThan(0);
    }
  });

  test("verify each plugin has a source", async () => {
    const pluginSources = await pluginsPage.getPluginSources();

    expect(pluginSources.length).toBeGreaterThan(0);

    for (const source of pluginSources) {
      expect(source.trim().length).toBeGreaterThan(0);
    }
  });

  test("verify plugin names and sources have matching counts", async () => {
    const pluginNames = await pluginsPage.getPluginNames();
    const pluginSources = await pluginsPage.getPluginSources();

    expect(pluginNames.length).toBe(pluginSources.length);
  });
});

test.describe("Plugins Pagination", () => {
  let pluginsPage: PluginsPage;

  test.beforeEach(({ page }) => {
    pluginsPage = new PluginsPage(page);
  });

  test("verify pagination controls navigate between pages", async () => {
    // Navigate to the plugins page with a small limit to ensure pagination
    await pluginsPage.navigateWithParams(5, 0);

    const page1Plugins = await pluginsPage.getPluginNames();

    // Verify we have plugins on the first page
    expect(page1Plugins.length).toBeGreaterThan(0);

    // Check if pagination controls exist (indicating there are multiple pages)
    const pagination = pluginsPage.page.locator('[data-scope="pagination"]');
    const paginationExists = await pagination.isVisible().catch(() => false);

    if (paginationExists) {
      // Check if page 2 button exists and is enabled
      const page2Button = pagination.getByRole("button", { name: /page 2/i });
      const page2ButtonExists = await page2Button.isVisible().catch(() => false);

      if (page2ButtonExists) {
        const isDisabled = await page2Button.isDisabled().catch(() => true);

        if (!isDisabled) {
          // Navigate to page 2
          await page2Button.click();
          await expect
            .poll(() => pluginsPage.getPluginNames(), { timeout: 30_000 })
            .not.toEqual(page1Plugins);

          const page2Plugins = await pluginsPage.getPluginNames();

          expect(page2Plugins.length).toBeGreaterThan(0);
          expect(page2Plugins).not.toEqual(page1Plugins);

          // Navigate back to page 1
          const page1Button = pagination.getByRole("button", { name: /page 1/i });

          await page1Button.click();
          await expect.poll(() => pluginsPage.getPluginNames(), { timeout: 30_000 }).toEqual(page1Plugins);
        }
      }
    }
  });

  test("verify pagination buttons functionality", async () => {
    await pluginsPage.navigateWithParams(5, 0);

    const initialPlugins = await pluginsPage.getPluginNames();

    expect(initialPlugins.length).toBeGreaterThan(0);

    // Check if next button is visible and enabled
    const nextButton = pluginsPage.paginationNextButton;
    const isNextButtonVisible = await nextButton.isVisible().catch(() => false);

    if (isNextButtonVisible) {
      const isNextButtonDisabled = await nextButton.isDisabled().catch(() => true);

      if (!isNextButtonDisabled) {
        // Click next button
        await nextButton.click();
        await pluginsPage.waitForTableData();

        const nextPagePlugins = await pluginsPage.getPluginNames();

        expect(nextPagePlugins).not.toEqual(initialPlugins);

        // Check if previous button is visible and enabled
        const prevButton = pluginsPage.paginationPrevButton;
        const isPrevButtonVisible = await prevButton.isVisible().catch(() => false);

        if (isPrevButtonVisible) {
          const isPrevButtonDisabled = await prevButton.isDisabled().catch(() => true);

          if (!isPrevButtonDisabled) {
            // Click previous button
            await prevButton.click();
            await pluginsPage.waitForTableData();

            const backToInitialPlugins = await pluginsPage.getPluginNames();

            expect(backToInitialPlugins).toEqual(initialPlugins);
          }
        }
      }
    }
  });
});
