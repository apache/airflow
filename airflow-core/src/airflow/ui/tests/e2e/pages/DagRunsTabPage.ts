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
  public readonly runsTable: Locator;
  public readonly tableRows: Locator;
  public readonly triggerButton: Locator;

  public constructor(page: Page) {
    super(page);
    this.markRunAsButton = page.getByTestId("mark-run-as-button").first();
    this.runsTable = page.getByTestId("table-list");
    this.tableRows = this.runsTable.locator("tbody").getByRole("row");
    this.triggerButton = page.getByTestId("trigger-dag-button");
  }

  private static escapeRegExp(value: string): string {
    return value.replaceAll(/[$()*+.?[\\\]^{|}]/g, "\\$&");
  }

  public async clickRunAndVerifyDetails(): Promise<void> {
    const firstRunLink = this.tableRows.first().getByRole("link").first();

    await expect(firstRunLink).toBeVisible({ timeout: 10_000 });
    await firstRunLink.click();
    await expect(this.page).toHaveURL(/.*\/dags\/.*\/runs\/[^/]+$/, { timeout: 15_000 });
    await expect(this.markRunAsButton).toBeVisible({ timeout: 10_000 });
  }

  public async clickRunsTab(): Promise<void> {
    const runsTab = this.page.getByRole("link", { exact: true, name: "Runs" });

    await expect(runsTab).toBeVisible({ timeout: 10_000 });
    await runsTab.click();
    await expect(this.page).toHaveURL(/.*\/dags\/[^/]+\/runs/, { timeout: 15_000 });
    await this.waitForRunsTableToLoad();
  }

  public async filterByState(state: string): Promise<void> {
    const currentUrl = new URL(this.page.url());

    currentUrl.searchParams.set("state", state.toLowerCase());

    await expect(async () => {
      await this.navigateTo(currentUrl.pathname + currentUrl.search);
      await expect(this.page).toHaveURL(/.*state=.*/, { timeout: 10_000 });
      await this.waitForRunsTableToLoad();
    }).toPass({ intervals: [2000], timeout: 30_000 });
  }

  public async markRunAs(state: "failed" | "success"): Promise<void> {
    const stateBadge = this.page.getByTestId("state-badge").first();

    await expect(stateBadge).toBeVisible({ timeout: 10_000 });
    const currentState = await stateBadge.textContent();

    if (currentState?.toLowerCase().includes(state)) {
      return;
    }

    await expect(this.markRunAsButton).toBeVisible({ timeout: 10_000 });
    await this.markRunAsButton.click();

    const stateOption = this.page.getByTestId(`mark-run-as-${state}`);

    await expect(stateOption).toBeVisible({ timeout: 5000 });

    if (await stateOption.isDisabled()) {
      await this.page.keyboard.press("Escape");

      return;
    }

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
    await expect(async () => {
      await this.navigateTo(`/dags/${dagId}`);
      await expect(this.page).toHaveURL(new RegExp(`/dags/${DagRunsTabPage.escapeRegExp(dagId)}`), {
        timeout: 5000,
      });
      await expect(this.triggerButton).toBeVisible({ timeout: 5000 });
    }).toPass({ intervals: [2000], timeout: 60_000 });
  }

  public async navigateToRunDetails(dagId: string, runId: string): Promise<void> {
    await expect(async () => {
      await this.navigateTo(`/dags/${dagId}/runs/${runId}`);
      await expect(this.page).toHaveURL(
        new RegExp(`/dags/${DagRunsTabPage.escapeRegExp(dagId)}/runs/${DagRunsTabPage.escapeRegExp(runId)}`),
        { timeout: 15_000 },
      );
      await expect(this.markRunAsButton).toBeVisible({ timeout: 15_000 });
    }).toPass({ intervals: [2000], timeout: 60_000 });
  }

  public async searchByRunIdPattern(pattern: string): Promise<void> {
    const currentUrl = new URL(this.page.url());

    currentUrl.searchParams.set("run_id_pattern", pattern);

    await expect(async () => {
      await this.navigateTo(currentUrl.pathname + currentUrl.search);
      await expect(this.page).toHaveURL(/.*run_id_pattern=.*/, { timeout: 10_000 });
      await this.waitForRunsTableToLoad();
    }).toPass({ intervals: [2000], timeout: 30_000 });
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

    await expect(rows).not.toHaveCount(0, { timeout: 10_000 });

    const nonMatchingRows = rows.filter({
      hasNot: this.page.getByTestId("state-badge").getByText(new RegExp(expectedState, "i")),
    });

    await expect(nonMatchingRows).toHaveCount(0, { timeout: 10_000 });
  }

  public async verifyRunDetailsDisplay(): Promise<void> {
    const firstRow = this.tableRows.first();

    await expect(firstRow).toBeVisible({ timeout: 10_000 });

    const runIdLink = firstRow.getByRole("link").first();

    await expect(runIdLink).toBeVisible();
    await expect(runIdLink).not.toBeEmpty();

    const stateBadge = firstRow.getByTestId("state-badge");

    await expect(stateBadge).toBeVisible();

    const timeElements = firstRow.locator("time");

    await expect(timeElements.first()).toBeVisible();
  }

  public async verifyRunsExist(): Promise<void> {
    const firstRow = this.tableRows.first();

    await expect(firstRow).toBeVisible({ timeout: 30_000 });
    await expect(this.tableRows).not.toHaveCount(0);
  }

  public async verifySearchResults(pattern: string): Promise<void> {
    await this.waitForRunsTableToLoad();

    const rows = this.tableRows;

    await expect(rows).not.toHaveCount(0, { timeout: 10_000 });

    const nonMatchingRows = rows.filter({
      hasNot: this.page.getByRole("link").getByText(new RegExp(pattern, "i")),
    });

    await expect(nonMatchingRows).toHaveCount(0, { timeout: 10_000 });
  }

  public async waitForRunsTableToLoad(): Promise<void> {
    await expect(this.runsTable).toBeVisible({ timeout: 30_000 });

    const firstRow = this.tableRows.first();
    const noDataMessage = this.page.getByText(/no.*dag.*runs.*found/i);

    await expect(firstRow.or(noDataMessage)).toBeVisible({ timeout: 30_000 });
  }
}
