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

export class BrowsePage extends BasePage {
  public readonly actionsTable: Locator;
  public readonly emptyStateMessage: Locator;
  public readonly pageHeading: Locator;

  public constructor(page: Page) {
    super(page);
    this.pageHeading = page.getByRole("heading", { name: /required action/i });
    this.actionsTable = page.getByTestId("table-list");
    this.emptyStateMessage = page.getByText(/no required actions found/i);
  }

  public static getRequiredActionsUrl(): string {
    return "/required_actions";
  }

  public async getActionsTableRowCount(): Promise<number> {
    const rows = this.page.locator("table tbody tr");
    const isTableVisible = await this.actionsTable.isVisible();

    if (!isTableVisible) {
      return 0;
    }

    return rows.count();
  }

  public async isEmptyStateDisplayed(): Promise<boolean> {
    return this.emptyStateMessage.isVisible();
  }

  public async isTableDisplayed(): Promise<boolean> {
    return this.actionsTable.isVisible();
  }

  public async navigateToRequiredActionsPage(): Promise<void> {
    await this.navigateTo(BrowsePage.getRequiredActionsUrl());
    await expect(this.pageHeading).toBeVisible({ timeout: 10_000 });
  }
}
