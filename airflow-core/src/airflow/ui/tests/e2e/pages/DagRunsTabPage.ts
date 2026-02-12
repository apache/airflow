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

export class DagRunsTabPage extends BasePage {
  public readonly markRunAsButton: Locator;
  public readonly nextPageButton: Locator;
  public readonly prevPageButton: Locator;
  public readonly runsTable: Locator;
  public readonly tableRows: Locator;
  public readonly triggerButton: Locator;

  private currentDagId?: string;
  private currentLimit?: number;

  public constructor(page: Page) {
    super(page);
    this.markRunAsButton = page.locator('[data-testid="mark-run-as-button"]').first();
    this.nextPageButton = page.locator('[data-testid="next"]');
    this.prevPageButton = page.locator('[data-testid="prev"]');
    this.runsTable = page.locator('[data-testid="table-list"]');
    this.tableRows = this.runsTable.locator("tbody tr");
    this.triggerButton = page.locator('[data-testid="trigger-dag-button"]');
  }

  public async clickNextPage(): Promise<void> {
    await this.waitForRunsTableToLoad();
    const firstRunLink = this.tableRows.first().locator("a[href*='/runs/']").first();

    await expect(firstRunLink).toBeVisible();
    const firstRunId = await firstRunLink.textContent();

    if (firstRunId === null || firstRunId === "") {
      throw new Error("Could not get first run ID before pagination");
    }

    await this.nextPageButton.click();
    await expect(this.tableRows.first()).not.toContainText(firstRunId, { timeout: 10_000 });
    await this.ensureUrlParams();
  }

  public async clickPrevPage(): Promise<void> {
    await this.waitForRunsTableToLoad();
    const firstRunLink = this.tableRows.first().locator("a[href*='/runs/']").first();

    await expect(firstRunLink).toBeVisible();
    const firstRunId = await firstRunLink.textContent();

    if (firstRunId === null || firstRunId === "") {
      throw new Error("Could not get first run ID before pagination");
    }

    await this.prevPageButton.click();
    await expect(this.tableRows.first()).not.toContainText(firstRunId, { timeout: 10_000 });
    await this.ensureUrlParams();
  }

  public async clickRunAndVerifyDetails(): Promise<void> {
    const firstRunLink = this.tableRows.first().locator("a[href*='/runs/']").first();

    await expect(firstRunLink).toBeVisible({ timeout: 10_000 });
    await firstRunLink.click();
    await this.page.waitForURL(/.*\/dags\/.*\/runs\/[^/]+$/, { timeout: 15_000 });
    await expect(this.markRunAsButton).toBeVisible({ timeout: 10_000 });
  }

  public async clickRunsTab(): Promise<void> {
    const runsTab = this.page.locator('a[href$="/runs"]');

    await expect(runsTab).toBeVisible({ timeout: 10_000 });
    await runsTab.click();
    await this.page.waitForURL(/.*\/dags\/[^/]+\/runs/, { timeout: 15_000 });
    await this.waitForRunsTableToLoad();
  }

  public async clickRunsTabWithPageSize(dagId: string, pageSize: number): Promise<void> {
    this.currentDagId = dagId;
    this.currentLimit = pageSize;

    await this.navigateTo(`/dags/${dagId}/runs?offset=0&limit=${pageSize}`);
    await this.page.waitForURL(/.*\/dags\/[^/]+\/runs.*offset=0&limit=/, {
      timeout: 15_000,
    });
    await this.waitForRunsTableToLoad();
  }

  public async filterByState(state: string): Promise<void> {
    const currentUrl = new URL(this.page.url());

    currentUrl.searchParams.set("state", state.toLowerCase());
    await this.navigateTo(currentUrl.pathname + currentUrl.search);
    await this.page.waitForURL(/.*state=.*/, { timeout: 15_000 });
    await this.waitForRunsTableToLoad();
  }

  public async getRowCount(): Promise<number> {
    await this.waitForRunsTableToLoad();

    return this.tableRows.count();
  }

  public async markRunAs(state: "failed" | "success"): Promise<void> {
    const stateBadge = this.page.locator('[data-testid="state-badge"]').first();

    await expect(stateBadge).toBeVisible({ timeout: 10_000 });
    const currentState = await stateBadge.textContent();

    if (currentState?.toLowerCase().includes(state)) {
      return;
    }

    await expect(this.markRunAsButton).toBeVisible({ timeout: 10_000 });
    await this.markRunAsButton.click();

    const stateOption = this.page.locator(`[data-testid="mark-run-as-${state}"]`);

    await expect(stateOption).toBeVisible({ timeout: 5000 });
    await stateOption.click();

    const confirmButton = this.page.getByRole("button", { name: "Confirm" });

    await expect(confirmButton).toBeVisible({ timeout: 5000 });

    const responsePromise = this.page.waitForResponse(
      (response) => response.url().includes("dagRuns") && response.request().method() === "PATCH",
      { timeout: 10_000 },
    );

    await confirmButton.click();
    await responsePromise;

    await expect(confirmButton).toBeHidden({ timeout: 10_000 });
  }

  public async navigateToDag(dagId: string): Promise<void> {
    await this.navigateTo(`/dags/${dagId}`);
    await this.page.waitForURL(`**/dags/${dagId}**`, { timeout: 15_000 });
    await expect(this.triggerButton).toBeVisible({ timeout: 10_000 });
  }

  public async navigateToRunDetails(dagId: string, runId: string): Promise<void> {
    await this.navigateTo(`/dags/${dagId}/runs/${runId}`);
    await this.page.waitForURL(`**/dags/${dagId}/runs/${runId}**`, { timeout: 15_000 });
    await expect(this.markRunAsButton).toBeVisible({ timeout: 15_000 });
  }

  public async searchByRunIdPattern(pattern: string): Promise<void> {
    const currentUrl = new URL(this.page.url());

    currentUrl.searchParams.set("run_id_pattern", pattern);
    await this.navigateTo(currentUrl.pathname + currentUrl.search);
    await this.page.waitForURL(/.*run_id_pattern=.*/, { timeout: 15_000 });
    await this.waitForRunsTableToLoad();
  }

  public async triggerDagRun(): Promise<string | undefined> {
    await expect(this.triggerButton).toBeVisible({ timeout: 10_000 });
    await this.triggerButton.click();

    const dialog = this.page.getByRole("dialog");

    await expect(dialog).toBeVisible({ timeout: 8000 });

    const confirmButton = dialog.getByRole("button", { name: "Trigger" });

    await expect(confirmButton).toBeVisible({ timeout: 5000 });

    const responsePromise = this.page.waitForResponse(
      (response) => {
        const url = response.url();
        const method = response.request().method();

        return method === "POST" && url.includes("dagRuns") && !url.includes("hitlDetails");
      },
      { timeout: 15_000 },
    );

    await confirmButton.click();

    const apiResponse = await responsePromise;

    const responseBody = await apiResponse.text();
    const responseJson = JSON.parse(responseBody) as { dag_run_id?: string };

    return responseJson.dag_run_id;
  }

  public async verifyFilteredByState(expectedState: string): Promise<void> {
    await this.waitForRunsTableToLoad();

    const rows = this.tableRows;

    await expect(rows).not.toHaveCount(0);

    const rowCount = await rows.count();

    for (let i = 0; i < Math.min(rowCount, 5); i++) {
      const stateBadge = rows.nth(i).locator('[data-testid="state-badge"]');

      await expect(stateBadge).toBeVisible();
      await expect(stateBadge).toContainText(expectedState, { ignoreCase: true });
    }
  }

  public async verifyRunDetailsDisplay(): Promise<void> {
    const firstRow = this.tableRows.first();

    await expect(firstRow).toBeVisible({ timeout: 10_000 });

    const runIdLink = firstRow.locator("a[href*='/runs/']").first();

    await expect(runIdLink).toBeVisible();
    await expect(runIdLink).not.toBeEmpty();

    const stateBadge = firstRow.locator('[data-testid="state-badge"]');

    await expect(stateBadge).toBeVisible();

    const timeElements = firstRow.locator("time");

    await expect(timeElements.first()).toBeVisible();
  }

  public async verifyRunsExist(): Promise<void> {
    const runLinks = this.runsTable.locator("a[href*='/runs/']");

    await expect(runLinks.first()).toBeVisible({ timeout: 30_000 });
    await expect(runLinks).not.toHaveCount(0);
  }

  public async verifySearchResults(pattern: string): Promise<void> {
    await this.waitForRunsTableToLoad();

    const rows = this.tableRows;

    await expect(rows).not.toHaveCount(0);

    const count = await rows.count();

    for (let i = 0; i < Math.min(count, 5); i++) {
      const runIdLink = rows.nth(i).locator("a[href*='/runs/']").first();

      await expect(runIdLink).toContainText(pattern, { ignoreCase: true });
    }
  }

  public async waitForRunsTableToLoad(): Promise<void> {
    await expect(this.runsTable).toBeVisible({ timeout: 10_000 });

    const dataLink = this.runsTable.locator("a[href*='/runs/']").first();
    const noDataMessage = this.page.getByText(/no.*dag.*runs.*found/i);

    await expect(dataLink.or(noDataMessage)).toBeVisible({ timeout: 30_000 });
  }

  private async ensureUrlParams(): Promise<void> {
    if (this.currentLimit === undefined || this.currentDagId === undefined) {
      return;
    }

    const currentUrl = this.page.url();
    const url = new URL(currentUrl);
    const hasLimit = url.searchParams.has("limit");
    const hasOffset = url.searchParams.has("offset");

    if (hasLimit && !hasOffset) {
      url.searchParams.set("offset", "0");
      await this.navigateTo(url.pathname + url.search);
      await this.waitForRunsTableToLoad();
    } else if (!hasLimit && !hasOffset) {
      url.searchParams.set("offset", "0");
      url.searchParams.set("limit", String(this.currentLimit));
      await this.navigateTo(url.pathname + url.search);
      await this.waitForRunsTableToLoad();
    }
  }
}
