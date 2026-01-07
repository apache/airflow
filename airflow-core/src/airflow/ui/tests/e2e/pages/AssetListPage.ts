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

  public async assetCount(): Promise<number> {
    return this.rows.count();
  }

  public async assetNames(): Promise<Array<string>> {
    return this.rows.locator("td a").allTextContents();
  }

  public async navigate(): Promise<void> {
    await this.navigateTo("/assets");
  }

  public async openFirstAsset(): Promise<string> {
    const link = this.rows.nth(0).locator("a").first();
    const name = await link.textContent();

    await link.click();

    return name?.trim() ?? "";
  }

  public async search(value: string): Promise<void> {
    await this.searchInput.fill(value);
  }

  public async waitForLoad(): Promise<void> {
    await this.heading.waitFor({ state: "visible", timeout: 30_000 });
  }
}
