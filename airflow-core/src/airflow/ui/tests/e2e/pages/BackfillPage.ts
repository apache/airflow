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
import { BasePage } from "tests/e2e/pages/BasePage";

export class BackfillPage extends BasePage {
  public readonly backfillBanner: Locator;
  public readonly backfillsTab: Locator;
  public readonly cancelButton: Locator;
  public readonly pauseButton: Locator;
  public readonly triggerButton: Locator;

  public constructor(page: Page) {
    super(page);
    this.backfillBanner = page.locator('div:has-text("Backfill in progress")');
    this.backfillsTab = page.locator('a[href*="/backfills"]');
    this.cancelButton = page.locator('button[aria-label*="ancel"]');
    this.pauseButton = page.locator('button[aria-label*="ause"]');
    this.triggerButton = page.locator('button:has-text("Trigger")');
  }

  public static getDagDetailUrl(dagName: string): string {
    return `/dags/${dagName}`;
  }

  public async clickCancelButton(): Promise<void> {
    await this.cancelButton.waitFor({ state: "visible", timeout: 10_000 });
    await this.cancelButton.click();
    await this.page.waitForTimeout(1000);
  }

  public async clickPauseButton(): Promise<void> {
    await this.pauseButton.waitFor({ state: "visible", timeout: 10_000 });
    await this.pauseButton.click();
    await this.page.waitForTimeout(1000);
  }

  public async createBackfill(dagName: string): Promise<void> {
    await this.navigateTo(BackfillPage.getDagDetailUrl(dagName));

    await this.triggerButton.waitFor({ state: "visible", timeout: 10_000 });
    await this.triggerButton.click();

    await this.page.waitForTimeout(1000);

    const backfillRadio = this.page.locator('input[value="backfill"]');

    await backfillRadio.waitFor({ state: "visible", timeout: 10_000 });
    await backfillRadio.click();

    const fromDateInput = this.page.locator('input[name="from_date"]');
    const toDateInput = this.page.locator('input[name="to_date"]');

    await fromDateInput.fill("2024-01-01T00:00");
    await toDateInput.fill("2024-01-02T00:00");

    await this.page.waitForTimeout(2000);

    const runButton = this.page.locator('button:has-text("Run")').last();

    await runButton.click({ force: true });

    await this.page.waitForTimeout(2000);
  }

  public async isBackfillPaused(): Promise<boolean> {
    await this.pauseButton.waitFor({ state: "visible", timeout: 10_000 });
    const ariaLabel = await this.pauseButton.getAttribute("aria-label");

    return Boolean(ariaLabel?.toLowerCase().includes("unpause"));
  }

  public async navigateToBackfillsTab(dagName: string): Promise<void> {
    await this.navigateTo(BackfillPage.getDagDetailUrl(dagName));
    await this.backfillsTab.waitFor({ state: "visible", timeout: 10_000 });
    await this.backfillsTab.click();
    await this.waitForPageLoad();
  }

  public async navigateToDagDetail(dagName: string): Promise<void> {
    await this.navigateTo(BackfillPage.getDagDetailUrl(dagName));
  }

  public async waitForBackfillCompletion(timeout: number = 30_000): Promise<void> {
    await this.backfillBanner.waitFor({ state: "hidden", timeout });
  }
}
