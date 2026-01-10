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

/**
 * Events/Audit Log Page Object
 */
export class EventsPage extends BasePage {
  // Locators
  public readonly eventColumn: Locator;
  public readonly eventsTable: Locator;
  public readonly extraColumn: Locator;
  public readonly ownerColumn: Locator;
  public readonly paginationNextButton: Locator;
  public readonly paginationPrevButton: Locator;
  public readonly tableRows: Locator;
  public readonly whenColumn: Locator;

  private currentDagId?: string;
  private currentLimit?: number;

  public constructor(page: Page) {
    super(page);
    this.eventsTable = page.locator('[data-testid="table-list"]');
    this.eventColumn = this.eventsTable.locator('th:has-text("Event")');
    this.extraColumn = this.eventsTable.locator('th:has-text("Extra")');
    this.ownerColumn = this.eventsTable.locator('th:has-text("User")');
    this.paginationNextButton = page.locator('[data-testid="next"]');
    this.paginationPrevButton = page.locator('[data-testid="prev"]');
    this.tableRows = this.eventsTable.locator("tbody tr");
    this.whenColumn = this.eventsTable.locator('th:has-text("When")');
  }

  public static getEventsUrl(dagId: string): string {
    return `/dags/${dagId}/events`;
  }

  public async clickColumnToSort(columnName: "Event" | "User" | "When"): Promise<void> {
    const columnHeader = this.eventsTable.locator(`th:has-text("${columnName}")`);
    const sortButton = columnHeader.locator('button[aria-label="sort"]');

    await sortButton.click();
    await this.waitForTableLoad();
    await this.ensureUrlParams();
  }

  public async clickNextPage(): Promise<void> {
    await this.paginationNextButton.click();
    await this.waitForTableLoad();
    await this.ensureUrlParams();
  }

  public async clickPrevPage(): Promise<void> {
    await this.paginationPrevButton.click();
    await this.waitForTableLoad();
    await this.ensureUrlParams();
  }

  public async getEventLogRows(): Promise<Array<Locator>> {
    const count = await this.tableRows.count();

    if (count === 0) {
      return [];
    }

    return this.tableRows.all();
  }

  /**
   * Get event types from the current page or all pages
   */
  public async getEventTypes(allPages: boolean = false): Promise<Array<string>> {
    const rows = await this.getEventLogRows();

    if (rows.length === 0) {
      return [];
    }

    const eventTypes: Array<string> = [];

    for (const row of rows) {
      const cells = row.locator("td");
      const eventCell = cells.nth(1);
      const text = await eventCell.textContent();

      if (text !== null) {
        eventTypes.push(text.trim());
      }
    }

    if (!allPages) {
      return eventTypes;
    }

    const allEventTypes = [...eventTypes];

    while (await this.hasNextPage()) {
      await this.clickNextPage();
      const pageEvents = await this.getEventTypes(false);

      allEventTypes.push(...pageEvents);
    }

    while ((await this.paginationPrevButton.count()) > 0 && (await this.paginationPrevButton.isEnabled())) {
      await this.clickPrevPage();
    }

    return allEventTypes;
  }

  public async hasNextPage(): Promise<boolean> {
    const count = await this.paginationNextButton.count();

    if (count === 0) {
      return false;
    }

    return await this.paginationNextButton.isEnabled();
  }

  public async navigateToAuditLog(dagId: string, limit?: number): Promise<void> {
    this.currentDagId = dagId;
    this.currentLimit = limit;

    const baseUrl = EventsPage.getEventsUrl(dagId);
    const url = limit === undefined ? baseUrl : `${baseUrl}?offset=0&limit=${limit}`;

    await this.page.goto(url, {
      timeout: 30_000,
      waitUntil: "domcontentloaded",
    });
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

  /**
   * Ensure offset=0 is present when limit is set to prevent limit from being ignored
   */
  private async ensureUrlParams(): Promise<void> {
    if (this.currentLimit === undefined || this.currentDagId === undefined) {
      return;
    }

    const currentUrl = this.page.url();
    const url = new URL(currentUrl);
    const hasLimit = url.searchParams.has("limit");
    const hasOffset = url.searchParams.has("offset");

    if (hasLimit && !hasOffset) {
      url.searchParams.set("offset", "0");
      await this.page.goto(url.toString(), {
        timeout: 30_000,
        waitUntil: "domcontentloaded",
      });
      await this.waitForTableLoad();
    } else if (!hasLimit && !hasOffset) {
      url.searchParams.set("offset", "0");
      url.searchParams.set("limit", String(this.currentLimit));
      await this.page.goto(url.toString(), {
        timeout: 30_000,
        waitUntil: "domcontentloaded",
      });
      await this.waitForTableLoad();
    }
  }
}
