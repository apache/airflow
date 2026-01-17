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

export class RequiredActionsPage extends BasePage {
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
    const texts = await rows.allTextContents();

    return texts.map((text) => text.trim()).filter((text) => text !== "");
  }

  public async hasNextPage(): Promise<boolean> {
    const isDisabled = await this.paginationNextButton.isDisabled();

    return !isDisabled;
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

  public async navigateToRequiredActionsPage(limit?: number): Promise<void> {
    await (limit === undefined
      ? this.navigateTo(RequiredActionsPage.getRequiredActionsUrl())
      : this.navigateTo(`${RequiredActionsPage.getRequiredActionsUrl()}?limit=${limit}&offset=0`));
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

      // Verify pending Required Actions exist
      await this.navigateToRequiredActionsPage();
      await this.page.getByTestId("filter-bar-add-button").click();
      await this.page.getByLabel("Filter", { exact: true }).getByText("Required Action State").click();
      await this.page.getByTestId("select-filter-trigger").click();
      await this.page.getByText("Pending").click();

      // Verify table has pending actions (not empty)
      await expect(this.actionsTable).toBeVisible({ timeout: 10_000 });
      const tableRows = this.page.locator("table tbody tr");

      await expect(tableRows.first()).toBeVisible({ timeout: 30_000 });
    }
  }

  public async verifyPagination(limit: number): Promise<void> {
    await this.navigateTo(`${RequiredActionsPage.getRequiredActionsUrl()}?offset=0&limit=${limit}`);
    await expect(this.page).toHaveURL(/.*limit=/, { timeout: 10_000 });
    await expect(this.actionsTable).toBeVisible({ timeout: 10_000 });

    const tableRows = this.page.locator("table tbody tr");

    await expect(tableRows.first()).toBeVisible({ timeout: 30_000 });
    expect(await tableRows.count()).toBeGreaterThan(0);

    const paginationNav = this.page.locator('nav[aria-label="pagination"], [role="navigation"]');

    await expect(paginationNav.first()).toBeVisible({ timeout: 10_000 });

    const page1Button = this.page.getByRole("button", { name: /page 1|^1$/ });

    await expect(page1Button.first()).toBeVisible({ timeout: 5000 });

    const page2Button = this.page.getByRole("button", { name: /page 2|^2$/ });
    const hasPage2 = await page2Button
      .first()
      .isVisible()
      .catch(() => false);

    if (hasPage2) {
      await page2Button.first().click();
      await expect(this.actionsTable).toBeVisible({ timeout: 10_000 });

      const tableRowsPage2 = this.page.locator("table tbody tr");
      const noDataMessage = this.page.locator("text=/no.*data|no.*actions|no.*results/i");

      await expect(tableRowsPage2.first().or(noDataMessage.first())).toBeVisible({ timeout: 30_000 });
    }
  }
}
