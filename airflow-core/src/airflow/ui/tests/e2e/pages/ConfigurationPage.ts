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

export class ConfigurationPage extends BasePage {
  public readonly heading: Locator;
  public readonly rows: Locator;
  public readonly table: Locator;

  public constructor(page: Page) {
    super(page);

    this.heading = page.getByRole("heading", {
      name: /config/i,
    });
    this.table = page.getByTestId("table-list");
    this.rows = this.table.locator("tbody tr").filter({
      has: page.locator("td"),
    });
  }

  public async navigate(): Promise<void> {
    await expect(async () => {
      await this.navigateTo("/configs");
      await this.page.waitForURL(/.*configs/, { timeout: 10_000 });
      await this.waitForLoad();
    }).toPass({ intervals: [2000], timeout: 60_000 });
  }

  public async waitForLoad(): Promise<void> {
    await expect(this.table).toBeVisible();
    await expect(this.rows.first()).toBeVisible();
  }
}
