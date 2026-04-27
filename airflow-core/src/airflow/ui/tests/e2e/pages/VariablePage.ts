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

export class VariablePage extends BasePage {
  public readonly addButton: Locator;
  public readonly importButton: Locator;
  public readonly searchInput: Locator;
  public readonly selectAllCheckbox: Locator;
  public readonly table: Locator;
  public readonly tableRows: Locator;

  public constructor(page: Page) {
    super(page);

    this.searchInput = page.getByTestId("search-dags");
    this.addButton = page.getByRole("button", { name: /add/i });
    this.importButton = page.getByRole("button", { name: "Import Variables" });
    this.table = page.getByTestId("table-list");
    this.tableRows = this.table.locator("tbody tr");
    this.selectAllCheckbox = page.locator("thead input[type='checkbox']");
  }

  public async getVariableKeys(): Promise<Array<string>> {
    await this.waitForLoad();
    const count = await this.tableRows.count();

    if (count === 0) {
      return [];
    }
    const keys = await this.tableRows.locator("td:nth-child(2)").allTextContents();

    return keys.map((key) => key.trim()).filter(Boolean);
  }

  public async navigate(): Promise<void> {
    await expect(async () => {
      await this.navigateTo("/variables");
      await this.page.waitForURL(/.*variables/, { timeout: 10_000 });
    }).toPass({ intervals: [2000], timeout: 60_000 });
  }

  public rowByKey(key: string): Locator {
    return this.page.locator("tr").filter({ hasText: key });
  }

  public async search(key: string): Promise<void> {
    await this.searchInput.fill(key);
  }

  public async selectRow(key: string): Promise<void> {
    const row = this.rowByKey(key);
    const checkbox = row.getByRole("checkbox");

    await expect(checkbox).toBeVisible({ timeout: 30_000 });
    // Use force:true because in Firefox the <label> overlay intercepts pointer events
    await checkbox.click({ force: true });
  }

  public async waitForLoad(): Promise<void> {
    await expect(this.table).toBeVisible({ timeout: 15_000 });

    // Wait for table data using Playwright locators instead of document.querySelector.
    const firstRow = this.tableRows.first();
    const emptyMessage = this.page.getByText(/no variables found/i);

    await expect(firstRow.or(emptyMessage)).toBeVisible({ timeout: 60_000 });
  }
}
