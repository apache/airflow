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

export class DagRunsPage extends BasePage {
  public static get dagRunsUrl(): string {
    return "/dag_runs";
  }

  public readonly dagRunsTable: Locator;

  public constructor(page: Page) {
    super(page);
    this.dagRunsTable = page.locator('table, div[role="table"]');
  }

  /**
   * Navigate to DAG Runs page and wait for data to load
   */
  public async navigate(): Promise<void> {
    await this.navigateTo(DagRunsPage.dagRunsUrl);
    await this.page.waitForURL(/.*dag_runs/, { timeout: 15_000 });
    await this.dagRunsTable.waitFor({ state: "visible", timeout: 10_000 });

    const dataLink = this.dagRunsTable.locator("a[href*='/dags/']").first();
    const noDataMessage = this.page.locator('text="No Dag Runs found"');

    await expect(dataLink.or(noDataMessage)).toBeVisible({ timeout: 30_000 });
  }

  /**
   * Verify DAG ID filtering via URL parameters
   */
  public async verifyDagIdFiltering(dagIdPattern: string): Promise<void> {
    await this.navigateTo(`${DagRunsPage.dagRunsUrl}?dag_id_pattern=${encodeURIComponent(dagIdPattern)}`);
    await this.page.waitForURL(/.*dag_id_pattern=.*/, { timeout: 15_000 });
    await this.page.waitForLoadState("networkidle");

    const dataLinks = this.dagRunsTable.locator("a[href*='/dags/']");

    await expect(dataLinks.first()).toBeVisible({ timeout: 30_000 });
    await expect(this.dagRunsTable).toBeVisible();

    const rows = this.dagRunsTable.locator("tbody tr");
    const rowCount = await rows.count();

    expect(rowCount).toBeGreaterThan(0);

    for (let i = 0; i < Math.min(rowCount, 5); i++) {
      const dagIdLink = rows.nth(i).locator("a[href*='/dags/']").first();
      const dagIdText = await dagIdLink.textContent();

      expect(dagIdText).toBeTruthy();
      expect(dagIdText).toContain(dagIdPattern);
    }
  }

  /**
   * Verify that the table contains DAG run data
   */
  public async verifyDagRunsExist(): Promise<void> {
    const dataLinks = this.dagRunsTable.locator("a[href*='/dags/']");

    await expect(dataLinks.first()).toBeVisible({ timeout: 30_000 });
    expect(await dataLinks.count()).toBeGreaterThan(0);
  }

  /**
   * Verify pagination controls and navigation
   */
  public async verifyPagination(limit: number): Promise<void> {
    await this.navigateTo(`${DagRunsPage.dagRunsUrl}?offset=0&limit=${limit}`);
    await this.page.waitForURL(/.*limit=/, { timeout: 10_000 });
    await this.page.waitForLoadState("networkidle");
    await this.dagRunsTable.waitFor({ state: "visible", timeout: 10_000 });

    const dataLinks = this.dagRunsTable.locator("a[href*='/dags/']");

    await expect(dataLinks.first()).toBeVisible({ timeout: 30_000 });

    const rows = this.dagRunsTable.locator("tbody tr");

    expect(await rows.count()).toBeGreaterThan(0);

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
      await this.page.waitForLoadState("networkidle");
      await this.dagRunsTable.waitFor({ state: "visible", timeout: 10_000 });

      const dataLinksPage2 = this.dagRunsTable.locator("a[href*='/dags/']");
      const noDataMessage = this.page.locator("text=/no.*data|no.*runs|no.*results/i");

      await expect(dataLinksPage2.first().or(noDataMessage.first())).toBeVisible({ timeout: 30_000 });
    }
  }

  /**
   * Verify that run details are displayed in the table row
   */
  public async verifyRunDetailsDisplay(): Promise<void> {
    const firstRow = this.dagRunsTable.locator("tbody tr").first();

    await expect(firstRow).toBeVisible({ timeout: 10_000 });

    const dagIdLink = firstRow.locator("a[href*='/dags/']").first();

    await expect(dagIdLink).toBeVisible();
    expect((await dagIdLink.textContent())?.trim()).toBeTruthy();

    const runIdLink = firstRow.locator("a[href*='/runs/']").first();

    await expect(runIdLink).toBeVisible();
    expect((await runIdLink.textContent())?.trim()).toBeTruthy();

    const stateCell = firstRow.locator("td").filter({ hasText: /running|success|failed|queued/i });

    await expect(stateCell.first()).toBeVisible();

    const timeElements = firstRow.locator("time");

    if ((await timeElements.count()) > 0) {
      await expect(timeElements.first()).toBeVisible();
    } else {
      const cellTexts = await firstRow.locator("td").allTextContents();
      const hasDateFormat = cellTexts.some((text) =>
        /\d{4}(?:-\d{2}){2}|(?:\d{1,2}\/){2}\d{4}|(?:\d{1,2}:){2}\d{2}/.test(text),
      );

      expect(hasDateFormat).toBeTruthy();
    }
  }

  /**
   * Verify state filtering via URL parameters
   */
  public async verifyStateFiltering(expectedState: string): Promise<void> {
    await this.navigateTo(`${DagRunsPage.dagRunsUrl}?state=${expectedState.toLowerCase()}`);
    await this.page.waitForURL(/.*state=.*/, { timeout: 15_000 });
    await this.page.waitForLoadState("networkidle");

    const dataLinks = this.dagRunsTable.locator("a[href*='/dags/']");

    await expect(dataLinks.first()).toBeVisible({ timeout: 30_000 });
    await expect(this.dagRunsTable).toBeVisible();

    const rows = this.dagRunsTable.locator("tbody tr");
    const rowCount = await rows.count();

    expect(rowCount).toBeGreaterThan(0);

    for (let i = 0; i < rowCount; i++) {
      const rowText = await rows.nth(i).textContent();

      expect(rowText?.toLowerCase()).toContain(expectedState.toLowerCase());
    }
  }
}
