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

export class XComsPage extends BasePage {
  public static get xcomsUrl(): string {
    return "/xcoms";
  }

  public readonly collapseAllButton: Locator;
  public readonly expandAllButton: Locator;
  public readonly xcomsTable: Locator;

  public constructor(page: Page) {
    super(page);
    this.collapseAllButton = page.getByRole("button", { name: /collapse/i });
    this.expandAllButton = page.getByRole("button", { name: /expand/i });
    this.xcomsTable = page.locator('table, div[role="table"]');
  }

  public async navigate(): Promise<void> {
    await this.navigateTo(XComsPage.xcomsUrl);
    await this.page.waitForURL(/.*xcoms/, { timeout: 15_000 });
    await this.xcomsTable.waitFor({ state: "visible", timeout: 10_000 });

    const dataLink = this.xcomsTable.locator("a[href*='/dags/']").first();
    const noDataMessage = this.page.locator("text=/no.*xcom|no.*data|no.*entries/i");

    await expect(dataLink.or(noDataMessage)).toBeVisible({ timeout: 30_000 });
  }

  public async verifyDagDisplayNameFiltering(dagDisplayNamePattern: string): Promise<void> {
    await this.navigateTo(
      `${XComsPage.xcomsUrl}?dag_display_name_pattern=${encodeURIComponent(dagDisplayNamePattern)}`,
    );
    await this.page.waitForURL(/.*dag_display_name_pattern=.*/, { timeout: 15_000 });
    await this.page.waitForLoadState("networkidle");

    const dataLinks = this.xcomsTable.locator("a[href*='/dags/']");

    await expect(dataLinks.first()).toBeVisible({ timeout: 30_000 });
    await expect(this.xcomsTable).toBeVisible();

    const rows = this.xcomsTable.locator("tbody tr");
    const rowCount = await rows.count();

    expect(rowCount).toBeGreaterThan(0);

    for (let i = 0; i < Math.min(rowCount, 3); i++) {
      const dagIdLink = rows.nth(i).locator("a[href*='/dags/']").first();
      const dagDisplayName = await dagIdLink.textContent();

      expect(dagDisplayName?.toLowerCase()).toContain(dagDisplayNamePattern.toLowerCase());
    }
  }

  public async verifyExpandCollapse(): Promise<void> {
    await this.navigate();

    await expect(this.expandAllButton.first()).toBeVisible({ timeout: 5000 });
    await this.expandAllButton.first().click();
    await this.page.waitForTimeout(500);

    await expect(this.collapseAllButton.first()).toBeVisible({ timeout: 5000 });
    await this.collapseAllButton.first().click();
    await this.page.waitForTimeout(500);
  }

  public async verifyKeyPatternFiltering(keyPattern: string): Promise<void> {
    await this.navigateTo(`${XComsPage.xcomsUrl}?key_pattern=${encodeURIComponent(keyPattern)}`);
    await this.page.waitForURL(/.*key_pattern=.*/, { timeout: 15_000 });
    await this.page.waitForLoadState("networkidle");

    const dataLinks = this.xcomsTable.locator("a[href*='/dags/']");

    await expect(dataLinks.first()).toBeVisible({ timeout: 30_000 });
    await expect(this.xcomsTable).toBeVisible();

    const rows = this.xcomsTable.locator("tbody tr");
    const rowCount = await rows.count();

    expect(rowCount).toBeGreaterThan(0);

    for (let i = 0; i < Math.min(rowCount, 3); i++) {
      const keyCell = rows.nth(i).locator("td").first();
      const keyText = await keyCell.textContent();

      expect(keyText?.toLowerCase()).toContain(keyPattern.toLowerCase());
    }
  }

  public async verifyPagination(limit: number): Promise<void> {
    await this.navigateTo(`${XComsPage.xcomsUrl}?offset=0&limit=${limit}`);
    await this.page.waitForURL(/.*limit=/, { timeout: 10_000 });
    await this.page.waitForLoadState("networkidle");
    await this.xcomsTable.waitFor({ state: "visible", timeout: 10_000 });

    const dataLinks = this.xcomsTable.locator("a[href*='/dags/']");

    await expect(dataLinks.first()).toBeVisible({ timeout: 30_000 });

    const rows = this.xcomsTable.locator("tbody tr");

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
      await this.xcomsTable.waitFor({ state: "visible", timeout: 10_000 });

      const dataLinksPage2 = this.xcomsTable.locator("a[href*='/dags/']");
      const noDataMessage = this.page.locator("text=/no.*data|no.*xcom|no.*results/i");

      await expect(dataLinksPage2.first().or(noDataMessage.first())).toBeVisible({ timeout: 30_000 });
    }
  }

  public async verifyXComDetailsDisplay(): Promise<void> {
    const firstRow = this.xcomsTable.locator("tbody tr").first();

    await expect(firstRow).toBeVisible({ timeout: 10_000 });

    const dagIdLink = firstRow.locator("a[href*='/dags/']").first();

    await expect(dagIdLink).toBeVisible();
    expect((await dagIdLink.textContent())?.trim()).toBeTruthy();

    const runIdLink = firstRow.locator("a[href*='/runs/']").first();

    await expect(runIdLink).toBeVisible();
    expect((await runIdLink.textContent())?.trim()).toBeTruthy();

    const cells = await firstRow.locator("td").allTextContents();

    expect(cells.length).toBeGreaterThanOrEqual(4);
    expect(cells.some((cell) => cell.trim().length > 0)).toBeTruthy();
  }

  public async verifyXComsExist(): Promise<void> {
    const dataLinks = this.xcomsTable.locator("a[href*='/dags/']");

    await expect(dataLinks.first()).toBeVisible({ timeout: 30_000 });
    expect(await dataLinks.count()).toBeGreaterThan(0);
  }

  public async verifyXComValuesDisplayed(): Promise<void> {
    const firstRow = this.xcomsTable.locator("tbody tr").first();

    await expect(firstRow).toBeVisible({ timeout: 10_000 });

    const valueCell = firstRow.locator("td").last();

    await expect(valueCell).toBeVisible();

    const textContent = await valueCell.textContent();
    const hasTextContent = (textContent?.trim().length ?? 0) > 0;
    const hasWidgetContent = (await valueCell.locator("button, pre, code").count()) > 0;

    expect(hasTextContent || hasWidgetContent).toBeTruthy();
  }
}
