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

import { DATA_ROWS } from "tests/e2e/utils/ui/selectors";

import { BasePage } from "./BasePage";

export class DagBundlesPage extends BasePage {
  public readonly browseMenuButton: Locator;
  public readonly dagBundlesMenuItem: Locator;
  public readonly heading: Locator;
  public readonly rows: Locator;
  public readonly table: Locator;

  public constructor(page: Page) {
    super(page);

    this.browseMenuButton = page.getByRole("button", { name: /^browse$/i });
    // DataTable's row count is the page heading, so this reads "3 Dag Bundles" rather than
    // "Dag Bundles".
    this.heading = page.getByRole("heading", { name: /dag bundles/i });
    this.dagBundlesMenuItem = page.getByRole("menuitem", { name: /^dag bundles$/i });
    this.table = page.getByTestId("table-list");
    this.rows = this.table.locator(DATA_ROWS).filter({
      has: page.locator("td"),
    });
  }

  public activeCellAt(index: number): Locator {
    return this.cellAt(index, "active");
  }

  /**
   * A cell addressed by its column id rather than its position.
   *
   * `TableList` tags every cell `table-cell-${column.id}`, so this survives the team column that
   * `multi_team` inserts after the name, and a column hidden through the visibility menu that
   * appears once a table has more than five columns.
   */
  public cellAt(rowIndex: number, columnId: string): Locator {
    return this.rows.nth(rowIndex).getByTestId(`table-cell-${columnId}`);
  }

  public async getRowCount(): Promise<number> {
    return this.rows.count();
  }

  public importErrorsCellAt(index: number): Locator {
    return this.cellAt(index, "import_error_count");
  }

  public lastRefreshedCellAt(index: number): Locator {
    return this.cellAt(index, "last_refreshed");
  }

  public nameCellAt(index: number): Locator {
    return this.cellAt(index, "name");
  }

  public async navigate(): Promise<void> {
    await expect(async () => {
      await this.navigateTo("/dag_bundles");
      await this.waitForLoad();
    }).toPass({ intervals: [2000], timeout: 60_000 });
  }

  public async navigateFromBrowseMenu(): Promise<void> {
    await expect(async () => {
      await this.navigateTo("/");
      await this.browseMenuButton.click();
      await expect(this.dagBundlesMenuItem).toBeVisible();
    }).toPass({ intervals: [2000], timeout: 60_000 });
    await this.dagBundlesMenuItem.click();
  }

  public versionCellAt(index: number): Locator {
    return this.cellAt(index, "version");
  }

  public async waitForLoad(): Promise<void> {
    await expect(this.table).toBeVisible();
    await expect(this.rows.first()).toBeVisible();
    // While the query is in flight DataTable swaps in ten skeleton rows, which are real `tr`/`td`
    // elements with no text -- visible rows alone would let a test read an empty cell. A populated
    // name is the cheapest signal that the real page has landed.
    await expect(this.nameCellAt(0)).not.toBeEmpty();
  }
}
