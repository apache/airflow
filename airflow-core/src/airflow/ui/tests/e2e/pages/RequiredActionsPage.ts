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

import { HITLReviewDrawer } from "tests/e2e/components/HITLReviewDrawer";

import { BasePage } from "./BasePage";

export class RequiredActionsPage extends BasePage {
  public readonly actionsTable: Locator;
  public readonly hitlReviewDrawer: HITLReviewDrawer;
  public readonly pageHeading: Locator;

  public constructor(page: Page) {
    super(page);
    this.pageHeading = page.getByRole("heading").filter({ hasText: /required action/i });
    this.actionsTable = page.getByTestId("table-list");
    this.hitlReviewDrawer = new HITLReviewDrawer(page);
  }

  public static getPendingRequiredActionsUrl(): string {
    return `${this.getRequiredActionsUrl()}?response_received=false`;
  }

  public static getRequiredActionsUrl(): string {
    return "/required_actions";
  }

  public async clickReviewDrawerButton(dagId: string): Promise<void> {
    const actionRow = this.getActionRow(dagId);
    const reviewDrawerButton = actionRow.getByRole("button", { name: "Open Review Drawer" });

    await expect(reviewDrawerButton).toBeVisible({ timeout: 30_000 });
    await reviewDrawerButton.click();
  }

  public getActionRow(dagId: string): Locator {
    return this.actionsTable
      .getByRole("row")
      .filter({ has: this.page.getByRole("cell", { name: dagId }) })
      .first();
  }

  public async navigateToPendingRequiredActionsPage(): Promise<void> {
    await this.navigateTo(RequiredActionsPage.getPendingRequiredActionsUrl());
    await expect(this.pageHeading).toBeVisible({ timeout: 30_000 });
  }

  public async navigateToRequiredActionsPage(): Promise<void> {
    await expect(async () => {
      await this.navigateTo(RequiredActionsPage.getRequiredActionsUrl());
      await expect(this.pageHeading).toBeVisible();
    }).toPass({ intervals: [2000], timeout: 60_000 });
  }
}
