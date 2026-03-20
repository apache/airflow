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

export class ConfigsPage extends BasePage {
  public readonly forbiddenStatus: Locator;
  public readonly heading: Locator;
  public readonly rows: Locator;
  public readonly table: Locator;

  public constructor(page: Page) {
    super(page);
    this.heading = page.getByRole("heading", { name: /configuration/i });
    this.table = page.getByTestId("table-list");
    this.forbiddenStatus = page.getByText(/403 forbidden/i);
    this.rows = this.table.locator("tbody tr").filter({
      has: page.locator("td"),
    });
  }

  public async getColumnNames(): Promise<Array<string>> {
    return this.table.locator("thead th").allTextContents();
  }

  public async getRowCount(): Promise<number> {
    return this.rows.count();
  }

  public async getRowDetails(index: number): Promise<{ key: string; section: string; value: string }> {
    const row = this.rows.nth(index);
    const cells = row.locator("td");

    const section = await cells.nth(0).textContent();
    const key = await cells.nth(1).textContent();
    const value = await cells.nth(2).textContent();

    return {
      key: (key ?? "").trim(),
      section: (section ?? "").trim(),
      value: (value ?? "").trim(),
    };
  }

  public async hasSectionAndKey(section: string, key: string): Promise<boolean> {
    const sectionLower = section.toLowerCase();
    const keyLower = key.toLowerCase();
    const rowCount = await this.getRowCount();

    for (let i = 0; i < rowCount; i++) {
      const row = await this.getRowDetails(i);

      if (row.section.toLowerCase() === sectionLower && row.key.toLowerCase() === keyLower) {
        return true;
      }
    }

    return false;
  }

  public async navigate(path = "/configs"): Promise<void> {
    await this.navigateTo(path);
  }

  public async waitForLoad(): Promise<void> {
    await this.heading.waitFor({ state: "visible", timeout: 30_000 });
    await this.page.waitForFunction(
      () => {
        const table = document.querySelector('[data-testid="table-list"]');
        const bodyText = document.body.textContent;

        return table !== null || bodyText.includes("403 Forbidden");
      },
      undefined,
      { timeout: 30_000 },
    );
  }
}
