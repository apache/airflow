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
import { expect } from "@playwright/test";
import type { Locator, Page } from "@playwright/test";

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
    this.tableRows = this.table.locator("tbody").getByRole("row");
    this.selectAllCheckbox = this.table.locator("thead").getByRole("checkbox");
  }

  public async getVariableKeys(): Promise<Array<string>> {
    await this.waitForLoad();
    const count = await this.tableRows.count();

    if (count === 0) {
      return [];
    }
    const keys = await this.tableRows.getByRole("cell").nth(1).allTextContents();

    return keys.map((key) => key.trim()).filter(Boolean);
  }

  public async navigate(): Promise<void> {
    await this.navigateTo("/variables");
  }

  public rowByKey(key: string): Locator {
    return this.tableRows.filter({ hasText: key });
  }

  public async search(key: string) {
    await this.searchInput.fill(key);
  }

  public async selectRow(key: string) {
    const row = this.rowByKey(key);

    await row.getByRole("checkbox").click();
  }

  public async waitForLoad(): Promise<void> {
    await expect(this.table).toBeVisible({ timeout: 15_000 });
    await this.waitForTableData();
  }

  private async waitForTableData(): Promise<void> {
    // Wait for either "No variables found" message or table rows with content
    const noVariablesMessage = this.page.getByText("No variables found");
    const firstRowCell = this.tableRows.first().getByRole("cell").nth(1);

    await expect(noVariablesMessage.or(firstRowCell)).toBeVisible({ timeout: 60_000 });
  }
}
