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

export class DagCalendarTab extends BasePage {
  public readonly dailyToggle: Locator;
  public readonly failedToggle = this.page.getByRole("button", { name: /failed/i });
  public readonly hourlyToggle: Locator;
  public readonly totalToggle = this.page.getByRole("button", { name: /total/i });

  public get activeCells(): Locator {
    return this.page.locator('[data-testid="calendar-cell"][data-has-data="true"]');
  }

  public get calendarCells(): Locator {
    return this.page.getByTestId("calendar-cell");
  }

  public get tooltip(): Locator {
    return this.page.getByTestId("calendar-tooltip");
  }

  public constructor(page: Page) {
    super(page);

    this.hourlyToggle = page.getByRole("button", { name: /hourly/i });
    this.dailyToggle = page.getByRole("button", { name: /daily/i });
  }

  public async getActiveCellColors(): Promise<Array<string>> {
    const count = await this.activeCells.count();
    const colors: Array<string> = [];

    for (let i = 0; i < count; i++) {
      const cell = this.activeCells.nth(i);
      const bg = await cell.evaluate((el) => window.getComputedStyle(el).backgroundColor);

      colors.push(bg);
    }

    return colors;
  }

  public async getActiveCellCount(): Promise<number> {
    return this.activeCells.count();
  }

  public async getManualRunStates(): Promise<Array<string>> {
    const count = await this.activeCells.count();
    const states: Array<string> = [];

    for (let i = 0; i < count; i++) {
      const cell = this.activeCells.nth(i);

      await cell.hover();
      await expect(this.tooltip).toBeVisible({ timeout: 20_000 });

      const text = ((await this.tooltip.textContent()) ?? "").toLowerCase();

      if (text.includes("success")) states.push("success");
      if (text.includes("failed")) states.push("failed");
      if (text.includes("running")) states.push("running");
    }

    return states;
  }

  public async navigateToCalendar(dagId: string) {
    await this.page.goto(`/dags/${dagId}/calendar`);
    await this.waitForCalendarReady();
  }

  public async switchToFailedView() {
    await this.failedToggle.click();
  }

  public async switchToHourly() {
    await this.hourlyToggle.click();

    await this.page.getByTestId("calendar-hourly-view").waitFor({ state: "visible", timeout: 30_000 });
  }

  public async switchToTotalView() {
    await this.totalToggle.click();
  }

  public async verifyMonthGridRendered() {
    await this.waitForCalendarReady();
  }

  private async waitForCalendarReady(): Promise<void> {
    await this.page.getByTestId("dag-calendar-root").waitFor({ state: "visible", timeout: 120_000 });

    await this.page.getByTestId("calendar-current-period").waitFor({ state: "visible", timeout: 120_000 });

    const overlay = this.page.getByTestId("calendar-loading-overlay");

    if (await overlay.isVisible().catch(() => false)) {
      await overlay.waitFor({ state: "hidden", timeout: 120_000 });
    }

    await this.page.getByTestId("calendar-grid").waitFor({ state: "visible", timeout: 120_000 });

    await this.page.waitForFunction(() => {
      const cells = document.querySelectorAll('[data-testid="calendar-cell"]');

      return cells.length > 0;
    });
  }
}
