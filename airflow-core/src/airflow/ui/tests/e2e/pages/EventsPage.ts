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
    this.eventsPageTitle = page.locator('h2:has-text("Audit Log")');
    this.eventsTable = page.locator('[data-testid="table-list"]');
    this.eventColumn = this.eventsTable.locator('th:has-text("Event")');
    this.extraColumn = this.eventsTable.locator('th:has-text("Extra")');
    this.filterBar = page
      .locator("div")
      .filter({ has: page.locator('button:has-text("Filter")') })
      .first();
    this.ownerColumn = this.eventsTable.locator('th:has-text("User")');
    this.tableRows = this.eventsTable.locator("tbody tr");
    this.whenColumn = this.eventsTable.locator('th:has-text("When")');
  }

  public static getEventsUrl(dagId: string): string {
    return `/dags/${dagId}/events`;
  }

  public async addFilter(filterName: string): Promise<void> {
    const filterButton = this.page.locator('button:has-text("Filter")');

    await filterButton.click();

    const filterMenu = this.page.locator('[role="menu"][data-state="open"]');

    await filterMenu.waitFor({ state: "visible", timeout: 5000 });

    const menuItem = filterMenu.locator(`[role="menuitem"]:has-text("${filterName}")`);

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
    return this.page.locator(`button:has-text("${filterLabel}:")`);
  }

  public async getTableRowCount(): Promise<number> {
    return this.tableRows.count();
  }

  public async navigate(): Promise<void> {
    await this.navigateTo("/events");
    await this.waitForTableLoad();
  }

  public async navigateToAuditLog(dagId: string): Promise<void> {
    await this.page.goto(EventsPage.getEventsUrl(dagId), {
      timeout: 30_000,
      waitUntil: "domcontentloaded",
    });
    await this.waitForTableLoad();
  }

  public async setFilterValue(filterLabel: string, value: string): Promise<void> {
    const filterPill = this.getFilterPill(filterLabel);

    if ((await filterPill.count()) > 0) {
      await filterPill.click();
    }

    // Wait for input to appear and fill it
    const filterInput = this.page.locator(`input[placeholder*="${filterLabel}" i], input`).last();

    await filterInput.waitFor({ state: "visible", timeout: 5000 });
    await filterInput.fill(value);
    await filterInput.press("Enter");
    await this.waitForTableLoad();
  }

  public async verifyLogEntriesWithData(): Promise<void> {
    const rows = await this.getEventLogRows();

    if (rows.length === 0) {
      throw new Error("No log entries found");
    }

    const [firstRow] = rows;

    if (!firstRow) {
      throw new Error("First row is undefined");
    }

    const whenCell = await this.getCellByColumnName(firstRow, "When");
    const eventCell = await this.getCellByColumnName(firstRow, "Event");
    const userCell = await this.getCellByColumnName(firstRow, "User");

    const whenText = await whenCell.textContent();
    const eventText = await eventCell.textContent();
    const userText = await userCell.textContent();

    expect(whenText?.trim()).toBeTruthy();
    expect(eventText?.trim()).toBeTruthy();
    expect(userText?.trim()).toBeTruthy();
  }

  public async verifyTableColumns(): Promise<void> {
    const headers = await this.eventsTable.locator("thead th").allTextContents();
    const expectedColumns = ["When", "Event", "User", "Extra"];

    for (const col of expectedColumns) {
      if (!headers.some((h) => h.toLowerCase().includes(col.toLowerCase()))) {
        throw new Error(`Expected column "${col}" not found in headers: ${headers.join(", ")}`);
      }
    }
  }

  public async waitForEventsTable(): Promise<void> {
    await this.waitForTableLoad();
  }

  /**
   * Wait for table to finish loading
   */
  public async waitForTableLoad(): Promise<void> {
    await this.eventsTable.waitFor({ state: "visible", timeout: 60_000 });

    await this.page.waitForFunction(
      () => {
        const table = document.querySelector('[data-testid="table-list"]');

        if (!table) {
          return false;
        }

        const skeletons = table.querySelectorAll('[data-scope="skeleton"]');

        if (skeletons.length > 0) {
          return false;
        }

        const rows = table.querySelectorAll("tbody tr");

        for (const row of rows) {
          const cells = row.querySelectorAll("td");
          let hasContent = false;

          for (const cell of cells) {
            if (cell.textContent && cell.textContent.trim().length > 0) {
              hasContent = true;
              break;
            }
          }

          if (!hasContent) {
            return false;
          }
        }

        return true;
      },
      undefined,
      { timeout: 60_000 },
    );
  }
}
