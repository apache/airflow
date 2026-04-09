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

export class PluginsPage extends BasePage {
  public readonly heading: Locator;
  public readonly nameColumn: Locator;
  public readonly rows: Locator;
  public readonly sourceColumn: Locator;
  public readonly table: Locator;

  public constructor(page: Page) {
    super(page);

    this.heading = page.getByRole("heading", { name: /plugins/i });
    this.table = page.getByTestId("table-list");
    this.rows = this.table.locator("tbody tr").filter({ has: page.locator("td") });
    this.nameColumn = this.rows.getByTestId("table-cell-name");
    this.sourceColumn = this.rows.getByTestId("table-cell-source");
  }

  public async navigate(): Promise<void> {
    await this.navigateTo("/plugins");
  }

  public async waitForLoad(): Promise<void> {
    await expect(this.table).toBeVisible({ timeout: 30_000 });
    await expect(this.rows.first()).toBeVisible({ timeout: 30_000 });
  }
}
