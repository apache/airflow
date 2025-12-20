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

export type ReprocessBehavior = "All Runs" | "Missing and Errored Runs" | "Missing Runs";

export type CreateBackfillOptions = {
  fromDate: string;
  reprocessBehavior: ReprocessBehavior;
  toDate: string;
};

export class BackfillPage extends BasePage {
  public readonly backfillDateError: Locator;
  public readonly backfillFromDateInput: Locator;
  public readonly backfillModeRadio: Locator;
  public readonly backfillRunButton: Locator;
  public readonly backfillsTable: Locator;
  public readonly backfillToDateInput: Locator;
  public readonly triggerButton: Locator;

  public constructor(page: Page) {
    super(page);
    this.triggerButton = page.locator('button[aria-label="Trigger Dag"]:has-text("Trigger")');
    this.backfillModeRadio = page.locator('label:has-text("Backfill")');
    this.backfillFromDateInput = page.locator('input[type="datetime-local"]').first();
    this.backfillToDateInput = page.locator('input[type="datetime-local"]').nth(1);
    this.backfillRunButton = page.locator('button:has-text("Run Backfill")');
    this.backfillsTable = page.locator("table");
    this.backfillDateError = page.locator('text="Start Date must be before the End Date"');
  }

  public static getBackfillsUrl(dagName: string): string {
    return `/dags/${dagName}/backfills`;
  }

  public static getDagDetailUrl(dagName: string): string {
    return `/dags/${dagName}`;
  }

  public async createBackfill(dagName: string, options: CreateBackfillOptions): Promise<void> {
    const { fromDate, reprocessBehavior, toDate } = options;

    await this.navigateToDagDetail(dagName);
    await this.openBackfillDialog();

    await this.backfillFromDateInput.fill(fromDate);
    await this.backfillToDateInput.fill(toDate);

    await this.selectReprocessBehavior(reprocessBehavior);

    await this.page.waitForTimeout(2000);

    await this.backfillRunButton.waitFor({ state: "visible", timeout: 5000 });
    await this.backfillRunButton.click();

    await this.page.waitForTimeout(2000);
  }

  public async getBackfillsTableRows(): Promise<number> {
    await this.page.waitForTimeout(1000);
    const rows = this.page.locator("table tbody tr");
    const count = await rows.count();

    return count;
  }

  public async isBackfillDateErrorVisible(): Promise<boolean> {
    return this.backfillDateError.isVisible();
  }

  public async navigateToBackfillsTab(dagName: string): Promise<void> {
    await this.navigateTo(BackfillPage.getBackfillsUrl(dagName));
  }

  public async navigateToDagDetail(dagName: string): Promise<void> {
    await this.navigateTo(BackfillPage.getDagDetailUrl(dagName));
  }

  public async openBackfillDialog(): Promise<void> {
    await this.triggerButton.waitFor({ state: "visible", timeout: 10_000 });
    await this.triggerButton.click();
    await this.page.waitForTimeout(1000);
    await this.backfillModeRadio.waitFor({ state: "visible", timeout: 8000 });
    await this.backfillModeRadio.click();
    await this.page.waitForTimeout(500);
  }

  public async selectReprocessBehavior(behavior: ReprocessBehavior): Promise<void> {
    const behaviorLabels: Record<ReprocessBehavior, string> = {
      "All Runs": "All Runs",
      "Missing and Errored Runs": "Missing and Errored Runs",
      "Missing Runs": "Missing Runs",
    };

    const label = behaviorLabels[behavior];
    const radioItem = this.page.locator(`label:has-text("${label}")`).first();

    await radioItem.waitFor({ state: "visible", timeout: 5000 });
    await radioItem.click();
  }
}
