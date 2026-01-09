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
import { DagsPage } from "./DagsPage";

export type MarkRunAsState = "failed" | "success";

export class BrowsePage extends BasePage {
  public readonly actionsTable: Locator;
  public readonly emptyStateMessage: Locator;
  public readonly pageHeading: Locator;
  public readonly paginationNextButton: Locator;
  public readonly paginationPrevButton: Locator;

  public constructor(page: Page) {
    super(page);
    this.pageHeading = page.getByRole("heading", { name: /required action/i });
    this.actionsTable = page.getByTestId("table-list");
    this.emptyStateMessage = page.getByText(/no required actions found/i);
    this.paginationNextButton = page.locator('[data-testid="next"]');
    this.paginationPrevButton = page.locator('[data-testid="prev"]');
  }

  public static getRequiredActionsUrl(): string {
    return "/required_actions";
  }

  public async clickNextPage(): Promise<void> {
    await this.paginationNextButton.click();
    await this.page.waitForLoadState("networkidle");
  }

  public async clickPrevPage(): Promise<void> {
    await this.paginationPrevButton.click();
    await this.page.waitForLoadState("networkidle");
  }

  public async getActionsTableRowCount(): Promise<number> {
    const rows = this.page.locator("table tbody tr");
    const isTableVisible = await this.actionsTable.isVisible();

    if (!isTableVisible) {
      return 0;
    }

    return rows.count();
  }

  public async getActionSubjects(): Promise<Array<string>> {
    const rows = this.page.locator("table tbody tr td:nth-child(2)");
    const count = await rows.count();
    const subjects: Array<string> = [];

    for (let i = 0; i < count; i++) {
      const text = await rows.nth(i).textContent();

      if (text !== null) {
        subjects.push(text.trim());
      }
    }

    return subjects;
  }

  public async isEmptyStateDisplayed(): Promise<boolean> {
    return this.emptyStateMessage.isVisible();
  }

  public async isPaginationVisible(): Promise<boolean> {
    return this.paginationNextButton.isVisible();
  }

  public async isTableDisplayed(): Promise<boolean> {
    return this.actionsTable.isVisible();
  }

  public async navigateToRequiredActionsPage(): Promise<void> {
    await this.navigateTo(BrowsePage.getRequiredActionsUrl());
    await expect(this.pageHeading).toBeVisible({ timeout: 10_000 });
  }

  public async triggerAndMarkDagRun(dagId: string, state: MarkRunAsState): Promise<void> {
    const dagsPage = new DagsPage(this.page);
    const dagRunId = await dagsPage.triggerDag(dagId);

    if (dagRunId !== null) {
      await this.page.goto(`/dags/${dagId}/runs/${dagRunId}`);

      const markDagRunButton = this.page.getByTestId("mark-run-as-button");

      await expect(markDagRunButton).toBeVisible({ timeout: 20_000 });
      await markDagRunButton.click();

      const stateOption = this.page.getByTestId(`mark-run-as-${state}`);

      await expect(stateOption).toBeVisible({ timeout: 20_000 });
      await stateOption.click();

      const confirmButton = this.page.getByTestId("mark-run-as-confirm");

      await expect(confirmButton).toBeVisible({ timeout: 20_000 });
      await confirmButton.click();

      await expect(confirmButton).not.toBeVisible({ timeout: 20_000 });

      const expectedState = state === "success" ? "Success" : "Failed";
      const stateBadge = this.page.getByTestId("state-badge").first();

      await expect(stateBadge).toBeVisible({ timeout: 20_000 });
      await expect(stateBadge).toContainText(expectedState, { timeout: 20_000 });
    }
  }
}
