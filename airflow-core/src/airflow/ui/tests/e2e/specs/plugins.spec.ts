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
    await pluginsPage.goto();
  });

  test.describe("Plugins List Display", () => {
    test("should display plugins list or empty state", async () => {
      const pluginCount = await pluginsPage.getPluginCount();

      if (pluginCount === 0) {
        // Verify empty state is displayed
        await pluginsPage.verifyEmptyState();
      } else {
        await expect(pluginsPage.pluginsTable).toBeVisible();
        expect(pluginCount).toBeGreaterThan(0);
      }
    });

    test("should display plugin information correctly when plugins exist", async () => {
      const pluginCount = await pluginsPage.getPluginCount();

      if (pluginCount === 0) {
        await pluginsPage.verifyEmptyState();
        return;
      }

      const pluginNames = await pluginsPage.getPluginNames();
      expect(pluginNames.length).toBeGreaterThan(0);
    });

    test("should display table headers", async () => {
      const pluginCount = await pluginsPage.getPluginCount();

      if (pluginCount === 0) {
        // Verify empty state instead of checking headers
        await pluginsPage.verifyEmptyState();
        return;
      }

      const headers = await pluginsPage.getTableHeaders();
      expect(headers.length).toBeGreaterThan(0);
    });
  });

  test.describe("Pagination", () => {
    test("should display pagination controls when there are multiple pages", async () => {
      const pluginCount = await pluginsPage.getPluginCount();

      // Skip if no plugins
      if (pluginCount === 0) {
        test.skip();

        return;
      }

      const paginationVisible = await pluginsPage.isPaginationVisible();

      // Pagination is visible when plugins exceed page size
      if (pluginCount > 50) {
        expect(paginationVisible).toBeTruthy();
      }
    });

    test("should navigate to next page", async () => {
      const nextPageEnabled = await pluginsPage.isNextPageEnabled();

      if (!nextPageEnabled) {
        test.skip();

        return;
      }

      const firstPagePlugins = await pluginsPage.getPluginNames();
      await pluginsPage.goToNextPage();
      const secondPagePlugins = await pluginsPage.getPluginNames();

      // Verify we're on a different page (different plugins)
      expect(firstPagePlugins).not.toEqual(secondPagePlugins);
    });

    test("should navigate to previous page", async () => {
      const nextPageEnabled = await pluginsPage.isNextPageEnabled();

      if (!nextPageEnabled) {
        test.skip();

        return;
      }

      // Go to second page
      await pluginsPage.goToNextPage();
      const secondPagePlugins = await pluginsPage.getPluginNames();

      // Go back to first page
      await pluginsPage.goToPreviousPage();
      const firstPagePlugins = await pluginsPage.getPluginNames();

      // Verify we're back on first page
      expect(firstPagePlugins).not.toEqual(secondPagePlugins);
    });
  });
});