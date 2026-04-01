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

export class ProvidersPage extends BasePage {
  public readonly adminMenuButton: Locator;
  public readonly heading: Locator;
  public readonly providersMenuItem: Locator;
  public readonly rows: Locator;
  public readonly table: Locator;

  public constructor(page: Page) {
    super(page);

    this.adminMenuButton = page.getByRole("button", { name: /^admin$/i });
    this.heading = page.getByRole("heading", { name: /^providers$/i });
    this.providersMenuItem = page.getByRole("menuitem", { name: /^providers$/i });
    this.table = page.getByTestId("table-list");
    this.rows = this.table.locator("tbody tr").filter({
      has: page.locator("td"),
    });
  }

  public descriptionCellAt(index: number): Locator {
    return this.rows.nth(index).locator("td").nth(2);
  }

  public async navigate(): Promise<void> {
    await this.navigateTo("/providers");
  }

  public async navigateFromAdminMenu(): Promise<void> {
    await this.navigateTo("/");
    await this.adminMenuButton.click();
    await expect(this.providersMenuItem).toBeVisible();
    await this.providersMenuItem.click();
  }

  public packageLinkAt(index: number): Locator {
    return this.rows.nth(index).locator("td").nth(0).getByRole("link");
  }

  public versionCellAt(index: number): Locator {
    return this.rows.nth(index).locator("td").nth(1);
  }

  public async waitForLoad(): Promise<void> {
    await expect(this.table).toBeVisible({ timeout: 30_000 });
    await expect(this.rows.first()).toBeVisible({ timeout: 30_000 });
    await expect(this.packageLinkAt(0)).toBeVisible({ timeout: 30_000 });
  }
}
