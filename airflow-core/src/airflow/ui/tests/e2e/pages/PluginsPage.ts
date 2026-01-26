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
import type { Locator, Page } from "@playwright/test";

import { BasePage } from "./BasePage";

/**
 * Plugins Page Object
 */
export class PluginsPage extends BasePage {
  public readonly emptyState: Locator;
  public readonly heading: Locator;
  public readonly paginationNextButton: Locator;
  public readonly paginationPrevButton: Locator;
  public readonly rows: Locator;
  public readonly table: Locator;

  public constructor(page: Page) {
    super(page);

    this.heading = page.getByRole("heading", {
      name: /plugins/i,
    });
    this.table = page.getByTestId("table-list");
    this.rows = this.table.locator("tbody tr").filter({
      has: page.locator("td"),
    });
    this.paginationNextButton = page.locator('[data-testid="next"]');
    this.paginationPrevButton = page.locator('[data-testid="prev"]');
    this.emptyState = page.getByText(/no items/i);
  }

  /**
   * Get the count of plugin rows
   */
  public async getPluginCount(): Promise<number> {
    return this.rows.count();
  }

  /**
   * Get all plugin names from the current page
   */
  public async getPluginNames(): Promise<Array<string>> {
    const count = await this.rows.count();

    if (count === 0) {
      return [];
    }

    return this.rows.locator("td:first-child").allTextContents();
  }

  /**
   * Get all plugin sources from the current page
   */
  public async getPluginSources(): Promise<Array<string>> {
    const count = await this.rows.count();

    if (count === 0) {
      return [];
    }

    return this.rows.locator("td:nth-child(2)").allTextContents();
  }

  /**
   * Navigate to Plugins page
   */
  public async navigate(): Promise<void> {
    await this.navigateTo("/plugins");
  }

  /**
   * Navigate to Plugins page with specific pagination parameters
   */
  public async navigateWithParams(limit: number, offset: number): Promise<void> {
    await this.navigateTo(`/plugins?limit=${limit}&offset=${offset}`);
    await this.waitForLoad();
  }

  /**
   * Wait for the plugins page to fully load
   */
  public async waitForLoad(): Promise<void> {
    await this.table.waitFor({ state: "visible", timeout: 30_000 });
    await this.waitForTableData();
  }

  /**
   * Wait for table data to be loaded (not skeleton loaders)
   */
  public async waitForTableData(): Promise<void> {
    // Wait for actual data cells to appear (not skeleton loaders)
    await this.page.waitForFunction(
      () => {
        const table = document.querySelector('[data-testid="table-list"]');

        if (!table) {
          return false;
        }

        // Check for actual content in tbody (real data, not skeleton)
        const cells = table.querySelectorAll("tbody tr td");

        return cells.length > 0;
      },
      undefined,
      { timeout: 30_000 },
    );
  }
}
