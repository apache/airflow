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
import { expect } from "@playwright/test";
import { BasePage } from "tests/e2e/pages/BasePage";

export class EventsPage extends BasePage {
  public readonly eventColumn: Locator;
  public readonly eventsPageTitle: Locator;
  public readonly eventsTable: Locator;
  public readonly extraColumn: Locator;
  public readonly filterBar: Locator;
  public readonly ownerColumn: Locator;
  public readonly tableRows: Locator;
  public readonly whenColumn: Locator;

  public constructor(page: Page) {
    super(page);
    this.eventsPageTitle = page.getByRole("heading", { level: 2, name: "Audit Log" });
    this.eventsTable = page.getByTestId("table-list");
    this.eventColumn = this.eventsTable.getByRole("columnheader").filter({ hasText: "Event" });
    this.extraColumn = this.eventsTable.getByRole("columnheader").filter({ hasText: "Extra" });
    this.filterBar = page
      .locator("div")
      .filter({ has: page.getByTestId("add-filter-button") })
      .first();
    this.ownerColumn = this.eventsTable.getByRole("columnheader").filter({ hasText: "User" });
    this.tableRows = this.eventsTable.locator("tbody").getByRole("row");
    this.whenColumn = this.eventsTable.getByRole("columnheader").filter({ hasText: "When" });
  }

  public static getEventsUrl(dagId: string): string {
    return `/dags/${dagId}/events`;
  }

  public async addFilter(filterName: string): Promise<void> {
    const filterButton = this.page.getByTestId("add-filter-button");

    await filterButton.click();

    const filterMenu = this.page.getByRole("menu");

    await expect(filterMenu).toBeVisible({ timeout: 10_000 });

    const menuItem = filterMenu.getByRole("menuitem", { name: filterName });

    await menuItem.click();
  }

  public async getCellByColumnName(row: Locator, columnName: string): Promise<Locator> {
    const headers = await this.eventsTable.locator("thead th").allTextContents();
    const index = headers.findIndex((h) => h.toLowerCase().includes(columnName.toLowerCase()));

    if (index === -1) {
      throw new Error(`Column "${columnName}" not found`);
    }

    return row.locator("td").nth(index);
  }

  public async getEventLogRows(): Promise<Array<Locator>> {
    const count = await this.tableRows.count();

    if (count === 0) {
      return [];
    }

    return this.tableRows.all();
  }

  public async getEventTypes(): Promise<Array<string>> {
    const rows = await this.getEventLogRows();

    if (rows.length === 0) {
      return [];
    }

    const eventTypes: Array<string> = [];

    for (const row of rows) {
      const eventCell = await this.getCellByColumnName(row, "Event");
      const text = await eventCell.textContent();

      if (text !== null) {
        eventTypes.push(text.trim());
      }
    }

    return eventTypes;
  }

  public getFilterPill(filterLabel: string): Locator {
    return this.page.getByRole("button", { name: `${filterLabel}:` });
  }

  public async getTableRowCount(): Promise<number> {
    return this.tableRows.count();
  }

  public async navigate(): Promise<void> {
    await this.navigateTo("/events");
    await this.waitForTableLoad();
  }

  public async navigateToAuditLog(dagId: string): Promise<void> {
    await expect(async () => {
      await this.page.goto(EventsPage.getEventsUrl(dagId), {
        timeout: 10_000,
        waitUntil: "domcontentloaded",
      });
      await this.eventsTable.waitFor({ state: "visible", timeout: 5000 });
    }).toPass({ intervals: [2000], timeout: 60_000 });
    await this.waitForTableLoad();
  }

  public async setFilterValue(filterLabel: string, value: string): Promise<void> {
    const filterPill = this.getFilterPill(filterLabel);

    if (await filterPill.isVisible()) {
      await filterPill.click();
    }

    const filterInput = this.page
      .locator("div")
      .filter({ has: this.page.getByText(`${filterLabel}:`) })
      .locator("input")
      .first();

    await expect(filterInput).toBeVisible({ timeout: 10_000 });
    await filterInput.fill(value);
    await filterInput.press("Enter");
    await this.waitForTableLoad();
  }

  public async verifyLogEntriesWithData(): Promise<void> {
    await expect(this.tableRows).not.toHaveCount(0);

    const firstRow = this.tableRows.first();
    const whenCell = await this.getCellByColumnName(firstRow, "When");
    const eventCell = await this.getCellByColumnName(firstRow, "Event");
    const userCell = await this.getCellByColumnName(firstRow, "User");

    await expect(whenCell).toHaveText(/\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}/);
    await expect(eventCell).toHaveText(/[a-z][_a-z]*/);
    await expect(userCell).toHaveText(/\w+/);
  }

  public async verifyTableColumns(): Promise<void> {
    await expect(this.whenColumn).toBeVisible();
    await expect(this.eventColumn).toBeVisible();
    await expect(this.ownerColumn).toBeVisible();
    await expect(this.extraColumn).toBeVisible();
  }

  public async waitForEventsTable(): Promise<void> {
    await this.waitForTableLoad();
  }

  /**
   * Wait for table to finish loading
   */
  public async waitForTableLoad(): Promise<void> {
    await expect(this.eventsTable).toBeVisible({ timeout: 60_000 });

    const skeleton = this.eventsTable.locator('[data-testid="skeleton"]');

    await expect(skeleton).toHaveCount(0, { timeout: 60_000 });

    const noDataMessage = this.page.getByText(/no.*events.*found/iu);

    await expect(async () => {
      const rowCount = await this.tableRows.count();

      await (rowCount > 0
        ? expect(this.tableRows.first().locator("td").first()).toHaveText(/.+/)
        : expect(noDataMessage).toBeVisible());
    }).toPass({ timeout: 60_000 });
  }
}
