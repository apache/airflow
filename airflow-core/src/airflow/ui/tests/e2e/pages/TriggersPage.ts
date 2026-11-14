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

import { BasePage } from "tests/e2e/pages/BasePage";
import { DATA_ROWS } from "tests/e2e/utils/ui/selectors";

export class TriggersPage extends BasePage {
  public readonly addFilterButton: Locator;
  public readonly tableRows: Locator;
  public readonly triggersTable: Locator;

  public constructor(page: Page) {
    super(page);
    this.addFilterButton = page.getByTestId("add-filter-button");
    this.triggersTable = page.getByTestId("table-list");
    this.tableRows = this.triggersTable.locator(DATA_ROWS);
  }

  /** Add one of the multiselect filters (Kind, Status) with a single option picked. */
  public async addFilter(filter: string, option: string): Promise<void> {
    await expect(this.addFilterButton).toBeVisible({ timeout: 30_000 });
    await this.addFilterButton.click();
    await this.page.getByRole("menu").getByRole("menuitem", { name: filter }).click();
    // The multiselect opens straight onto its options when added.
    await this.page.getByRole("option", { exact: true, name: option }).click();
    await this.page.keyboard.press("Escape");
  }

  /** The cells of one column, row by row, found by the column's header text. */
  public async columnTexts(header: string): Promise<Array<string>> {
    const headers = await this.triggersTable.locator("thead th").allTextContents();
    const index = headers.findIndex((text) => text.toLowerCase().includes(header.toLowerCase()));

    if (index === -1) {
      throw new Error(`Column "${header}" not found in ${headers.join(", ")}`);
    }

    return (await this.tableRows.locator(`td:nth-child(${index + 1})`).allTextContents()).map((text) =>
      text.trim(),
    );
  }

  public async navigate(search = ""): Promise<void> {
    await expect(async () => {
      await this.navigateTo(`/triggers${search}`);
      await expect(this.triggersTable).toBeVisible({ timeout: 30_000 });
    }).toPass({ intervals: [2000, 5000], timeout: 60_000 });
  }
}
