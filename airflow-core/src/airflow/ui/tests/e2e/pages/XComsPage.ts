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

  public readonly addFilterButton: Locator;
  public readonly collapseAllButton: Locator;
  public readonly expandAllButton: Locator;
  public readonly xcomsTable: Locator;

  public constructor(page: Page) {
    super(page);
    this.addFilterButton = page.locator('[data-testid="add-filter-button"]');
    this.collapseAllButton = page.locator('[data-testid="collapse-all-button"]');
    this.expandAllButton = page.locator('[data-testid="expand-all-button"]');
    this.xcomsTable = page.locator('[data-testid="table-list"]');
  }

  public async applyFilter(filterName: string, value: string): Promise<void> {
    await this.addFilterButton.click();

    const filterMenu = this.page.locator('[role="menu"]');

    await filterMenu.waitFor({ state: "visible", timeout: 5000 });

    const filterOption = filterMenu.locator('[role="menuitem"]').filter({ hasText: filterName });

    await filterOption.click();

    const filterPill = this.page
      .locator("div")
      .filter({ hasText: `${filterName}:` })
      .first();
    const filterInput = filterPill.locator("input");

    await filterInput.waitFor({ state: "visible", timeout: 5000 });
    await filterInput.fill(value);
    await filterInput.press("Enter");
    await this.page.waitForLoadState("networkidle");
  }

  public async navigate(): Promise<void> {
    await this.navigateTo(XComsPage.xcomsUrl);
    await this.page.waitForURL(/.*xcoms/, { timeout: 15_000 });
    await this.xcomsTable.waitFor({ state: "visible", timeout: 10_000 });
    await this.page.waitForLoadState("networkidle");
  }

  public async verifyDagDisplayNameFiltering(dagDisplayNamePattern: string): Promise<void> {
    await this.navigate();
    await this.applyFilter("DAG ID", dagDisplayNamePattern);

    await expect(async () => {
      const firstLink = this.xcomsTable.locator("tbody tr").first().locator("a[href*='/dags/']").first();

      await expect(firstLink).toContainText(dagDisplayNamePattern, { ignoreCase: true });
    }).toPass({ timeout: 30_000 });

    const rows = this.xcomsTable.locator("tbody tr");
    const rowCount = await rows.count();

    expect(rowCount).toBeGreaterThan(0);

    for (let i = 0; i < Math.min(rowCount, 3); i++) {
      const dagIdLink = rows.nth(i).locator("a[href*='/dags/']").first();

      await expect(dagIdLink).toContainText(dagDisplayNamePattern, { ignoreCase: true });
    }
  }

  public async verifyExpandCollapse(): Promise<void> {
    await this.navigate();

    await expect(this.expandAllButton.first()).toBeVisible({ timeout: 5000 });
    await this.expandAllButton.first().click();
    await this.page.waitForLoadState("networkidle");

    await expect(this.collapseAllButton.first()).toBeVisible({ timeout: 5000 });
    await this.collapseAllButton.first().click();
    await this.page.waitForLoadState("networkidle");
  }

  public async verifyKeyPatternFiltering(keyPattern: string): Promise<void> {
    await this.navigate();
    await this.applyFilter("Key", keyPattern);

    await expect(async () => {
      const firstKeyCell = this.xcomsTable.locator("tbody tr").first().locator("td").first();

      await expect(firstKeyCell).toContainText(keyPattern, { ignoreCase: true });
    }).toPass({ timeout: 30_000 });

    const rows = this.xcomsTable.locator("tbody tr");
    const rowCount = await rows.count();

    expect(rowCount).toBeGreaterThan(0);

    for (let i = 0; i < Math.min(rowCount, 3); i++) {
      const keyCell = rows.nth(i).locator("td").first();

      await expect(keyCell).toContainText(keyPattern, { ignoreCase: true });
    }
  }

  public async verifyXComDetailsDisplay(): Promise<void> {
    const firstRow = this.xcomsTable.locator("tbody tr").first();

    await expect(firstRow).toBeVisible({ timeout: 10_000 });

    const keyCell = firstRow.locator("td").first();

    await expect(async () => {
      await expect(keyCell).toBeVisible();
      const text = await keyCell.textContent();

      expect(text?.trim()).toBeTruthy();
    }).toPass({ timeout: 10_000 });

    const dagIdLink = firstRow.locator("a[href*='/dags/']").first();

    await expect(dagIdLink).toBeVisible();
    await expect(dagIdLink).not.toBeEmpty();

    const runIdLink = firstRow.locator("a[href*='/runs/']").first();

    await expect(runIdLink).toBeVisible();
    await expect(runIdLink).not.toBeEmpty();

    const taskIdLink = firstRow.locator("a[href*='/tasks/']").first();

    await expect(taskIdLink).toBeVisible();
    await expect(taskIdLink).not.toBeEmpty();
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

    await expect(async () => {
      const textContent = await valueCell.textContent();
      const hasTextContent = (textContent?.trim().length ?? 0) > 0;
      const hasWidgetContent = (await valueCell.locator("button, pre, code").count()) > 0;

      expect(hasTextContent || hasWidgetContent).toBeTruthy();
    }).toPass({ timeout: 10_000 });
  }
}
