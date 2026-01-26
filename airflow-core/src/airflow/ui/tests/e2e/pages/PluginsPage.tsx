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
import { expect, type Locator, type Page } from "@playwright/test";

export class PluginsPage {
  public readonly nextPageButton: Locator;
  public readonly noDataMessage: Locator;
  public readonly page: Page;
  public readonly pageTitle: Locator;
  public readonly paginationContainer: Locator;
  public readonly pluginRows: Locator;
  public readonly pluginsTable: Locator;
  public readonly prevPageButton: Locator;
  public readonly tableHeaders: Locator;

  public constructor(page: Page) {
    this.page = page;

    // Page elements
    this.pageTitle = page.locator('h1, [data-testid="page-title"]').filter({ hasText: /plugins/i });
    this.pluginsTable = page.locator("table");
    this.pluginRows = page.locator("tbody tr");
    this.tableHeaders = page.locator("thead th");

    // Pagination elements
    this.paginationContainer = page.locator('nav[aria-label*="pagination"], nav[data-scope="pagination"]');
    this.nextPageButton = page.locator('button[aria-label*="next page"], [data-testid="next"]');
    this.prevPageButton = page.locator('button[aria-label*="previous page"], [data-testid="prev"]');

    // Other elements
    this.noDataMessage = page.locator("text=/No Plugins found|No Plugins Found|noItemsFound/i");
  }

  // Get current page number
  public async getCurrentPageNumber(): Promise<number> {
    const activePageButton = this.page.locator('[aria-current="page"], .active').first();
    const pageText = await activePageButton.textContent();

    return pageText !== null && pageText !== "" ? Number.parseInt(pageText.trim(), 10) : 1;
  }

  // Get the count of visible plugins
  public async getPluginCount(): Promise<number> {
    await this.waitForPluginsToLoad();

    return await this.pluginRows.count();
  }

  // Get plugin names from the current page
  public async getPluginNames(): Promise<Array<string>> {
    await this.waitForPluginsToLoad();
    const count = await this.pluginRows.count();
    const names: Array<string> = [];

    for (const index of Array.from({ length: count }, (_, i) => i)) {
      const row = this.pluginRows.nth(index);
      const nameCell = row.locator("td").first();
      const name = await nameCell.textContent();

      if (name !== null && name !== "") {
        names.push(name.trim());
      }
    }

    return names;
  }

  // Get table headers
  public async getTableHeaders(): Promise<Array<string>> {
    const count = await this.tableHeaders.count();
    const headers: Array<string> = [];

    for (const index of Array.from({ length: count }, (_, i) => i)) {
      const headerText = await this.tableHeaders.nth(index).textContent();

      if (headerText !== null && headerText !== "") {
        headers.push(headerText.trim());
      }
    }

    return headers;
  }

  // Navigate to the plugins page
  public async goto(): Promise<void> {
    await this.page.goto("/plugins");
    await this.page.waitForLoadState("networkidle");
  }

  // Navigate to the next page
  public async goToNextPage(): Promise<void> {
    await this.nextPageButton.click();
    await this.page.waitForLoadState("networkidle");
    await this.waitForPluginsToLoad();
  }

  // Navigate to the previous page
  public async goToPreviousPage(): Promise<void> {
    await this.prevPageButton.click();
    await this.page.waitForLoadState("networkidle");
    await this.waitForPluginsToLoad();
  }

  // Check if next page button is enabled
  public async isNextPageEnabled(): Promise<boolean> {
    const isDisabled = await this.nextPageButton.isDisabled().catch(() => true);

    return !isDisabled;
  }

  // Check if pagination is visible
  public async isPaginationVisible(): Promise<boolean> {
    try {
      await this.paginationContainer.scrollIntoViewIfNeeded();

      return await this.paginationContainer.isVisible();
    } catch {
      return false;
    }
  }

  // Check if previous page button is enabled
  public async isPreviousPageEnabled(): Promise<boolean> {
    const isDisabled = await this.prevPageButton.isDisabled().catch(() => true);

    return !isDisabled;
  }

  // Verify empty state
  public async verifyEmptyState(): Promise<void> {
    await expect(this.noDataMessage).toBeVisible();
  }

  // Wait for the plugins list to load
  public async waitForPluginsToLoad(): Promise<void> {
    await this.page.waitForSelector("table tbody tr", {
      timeout: 8000,
    });
  }
}
