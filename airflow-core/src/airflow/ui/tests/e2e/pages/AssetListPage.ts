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

export class AssetListPage extends BasePage {
  public readonly emptyState: Locator;
  public readonly heading: Locator;
  public readonly rows: Locator;
  public readonly searchInput: Locator;
  public readonly table: Locator;

  public constructor(page: Page) {
    super(page);

    this.heading = page.getByRole("heading", {
      name: /\d+\s+asset/i,
    });
    this.table = page.getByTestId("table-list");
    this.rows = this.table.locator("tbody tr").filter({
      has: page.locator("td"),
    });

    this.searchInput = page.getByTestId("search-dags");
    this.emptyState = page.getByText(/no items/i);
  }

  public async navigate(): Promise<void> {
    await expect(async () => {
      await this.navigateTo("/assets");
      await this.page.waitForURL(/.*assets/, { timeout: 10_000 });
      await this.waitForLoad();
    }).toPass({ intervals: [2000], timeout: 60_000 });
  }

  public async openFirstAsset(): Promise<string> {
    const count = await this.rows.count();

    if (count === 0) {
      throw new Error("No assets found to click");
    }

    const link = this.rows.nth(0).locator("a").first();
    const name = await link.textContent();

    await link.click();

    return name?.trim() ?? "";
  }

  public async search(value: string): Promise<void> {
    await this.searchInput.fill(value);
    await this.waitForLoad();
  }

  public async waitForLoad(): Promise<void> {
    await expect(this.table).toBeVisible();
    await expect(this.rows.locator("td a").first()).toBeVisible();
  }
}
