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

import { BasePage } from "./BasePage";

export class ProvidersPage extends BasePage {
  public readonly heading: Locator;
  public readonly paginationNextButton: Locator;
  public readonly paginationPrevButton: Locator;
  public readonly rows: Locator;
  public readonly table: Locator;

  public constructor(page: Page) {
    super(page);

    this.heading = page.getByRole("heading", { name: /^providers$/i });
    this.table = page.getByTestId("table-list");
    this.rows = this.table.locator("tbody tr").filter({
      has: page.locator("td"),
    });
    this.paginationNextButton = page.locator('[data-testid="next"]');
    this.paginationPrevButton = page.locator('[data-testid="prev"]');
  }

  /**
   * Click next page button
   */
  public async clickNextPage(): Promise<void> {
    const initialProviderNames = await this.providerNames();

    await this.paginationNextButton.click();

    await expect.poll(() => this.providerNames(), { timeout: 10_000 }).not.toEqual(initialProviderNames);
    await this.waitForTableData();
  }

  /**
   * Click previous page button
   */
  public async clickPrevPage(): Promise<void> {
    const initialProviderNames = await this.providerNames();

    await this.paginationPrevButton.click();

    await expect.poll(() => this.providerNames(), { timeout: 10_000 }).not.toEqual(initialProviderNames);
    await this.waitForTableData();
  }

  public async getRowCount(): Promise<number> {
    return this.rows.count();
  }

  public async getRowDetails(index: number) {
    const row = this.rows.nth(index);
    const cells = row.locator("td");

    const pkg = await cells.nth(0).locator("a").textContent();
    const ver = await cells.nth(1).textContent();
    const desc = await cells.nth(2).textContent();

    return {
      description: (desc ?? "").trim(),
      packageName: (pkg ?? "").trim(),
      version: (ver ?? "").trim(),
    };
  }

  public async navigate(): Promise<void> {
    await this.navigateTo("/providers");
  }

  public async providerNames(): Promise<Array<string>> {
    return this.rows.locator("td a").allTextContents();
  }

  public async waitForLoad(): Promise<void> {
    await this.table.waitFor({ state: "visible", timeout: 30_000 });
    await this.waitForTableData();
  }

  private async waitForTableData(): Promise<void> {
    // Wait for actual data links to appear (not skeleton loaders)
    await this.page.waitForFunction(
      () => {
        const table = document.querySelector('[data-testid="table-list"]');

        if (!table) {
          return false;
        }

        // Check for actual links in tbody (real data, not skeleton)
        const links = table.querySelectorAll("tbody tr td a");

        return links.length > 0;
      },
      undefined,
      { timeout: 30_000 },
    );
  }
}
