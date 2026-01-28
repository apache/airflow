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

export class DagCalendarPage extends BasePage {
  public readonly calendarGrid: Locator;
  public readonly calendarTab: Locator;
  public readonly monthYearHeader: Locator;

  public constructor(page: Page) {
    super(page);
    this.calendarTab = page.getByRole("tab", { name: "Calendar" });
    this.calendarGrid = page
      .locator(".react-calendar-heatmap")
      .or(page.getByTestId("calendar-cell").first().locator(".."));
    this.monthYearHeader = page.getByTestId("calendar-header-date");
  }

  public async clickDay(date: string): Promise<void> {
    const cell = this.page.locator(`[data-testid="calendar-cell"][data-date="${date}"]`);

    await cell.click();
  }

  public async navigateToCalendar(dagId: string): Promise<void> {
    await this.page.goto(`/dags/${dagId}/calendar`);

    await this.page.waitForLoadState("networkidle");
  }

  public async verifyCalendarRender(): Promise<void> {
    await expect(this.monthYearHeader).toBeVisible();

    await expect(this.page.getByRole("button", { name: "Daily" })).toBeVisible();

    await expect(this.page.getByRole("button", { name: "Hourly" })).toBeVisible();
  }

  public async verifyDayRun(date: string, status: "failed" | "running" | "success"): Promise<void> {
    const cell = this.page.locator(`[data-testid="calendar-cell"][data-date="${date}"]`);

    await expect(cell).toBeVisible();

    await cell.hover();

    const tooltip = this.page.getByRole("tooltip");

    await expect(tooltip).toBeVisible();

    await expect(tooltip).toContainText(date);

    const statusText = status.charAt(0).toUpperCase() + status.slice(1);

    await expect(tooltip).toContainText(statusText);
  }
}
