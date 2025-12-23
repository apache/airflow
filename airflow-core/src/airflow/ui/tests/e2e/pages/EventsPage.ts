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
  public readonly auditLogTab: Locator;
  public readonly eventColumn: Locator;
  public readonly eventsTable: Locator;
  public readonly extraColumn: Locator;
  public readonly ownerColumn: Locator;
  public readonly paginationNextButton: Locator;
  public readonly paginationPrevButton: Locator;
  public readonly skeletonLoader: Locator;
  public readonly tableRows: Locator;
  public readonly whenColumn: Locator;

  public constructor(page: Page) {
    super(page);
    this.auditLogTab = page.locator('a[href$="/events"]');
    this.eventsTable = page.locator('[data-testid="table-list"]');
    this.eventColumn = this.eventsTable.locator('th:has-text("Event")');
    this.extraColumn = this.eventsTable.locator('th:has-text("Extra")');
    this.ownerColumn = this.eventsTable.locator('th:has-text("User")');
    this.paginationNextButton = page.locator('[data-testid="next"]');
    this.paginationPrevButton = page.locator('[data-testid="prev"]');
    this.skeletonLoader = page.locator('[data-testid="skeleton"]');
    this.tableRows = this.eventsTable.locator("tbody tr");
    this.whenColumn = this.eventsTable.locator('th:has-text("When")');
  }

  /**
   * Build URL for DAG-specific events page
   */
  public static getEventsUrl(dagId: string): string {
    return `/dags/${dagId}/events`;
  }

  /**
   * Click a column header to sort
   */
  public async clickColumnToSort(columnName: "Event" | "User" | "When"): Promise<void> {
    const columnHeader = this.eventsTable.locator(`th:has-text("${columnName}")`);
    const sortButton = columnHeader.locator('button[aria-label="sort"]');

    await sortButton.click();
    await this.page.waitForTimeout(500);
  }

  /**
   * Click next page button
   */
  public async clickNextPage(): Promise<void> {
    await this.paginationNextButton.click();
    await this.waitForPageLoad();
    await this.waitForTableLoad();
  }

  /**
   * Click previous page button
   */
  public async clickPrevPage(): Promise<void> {
    await this.paginationPrevButton.click();
    await this.waitForPageLoad();
    await this.waitForTableLoad();
  }

  /**
   * Get count of event log rows
   */
  public async getEventLogCount(): Promise<number> {
    try {
      return await this.tableRows.count();
    } catch {
      return 0;
    }
  }

  /**
   * Get all event log rows
   */
  public async getEventLogRows(): Promise<Array<Locator>> {
    try {
      const count = await this.tableRows.count();

      if (count === 0) {
        return [];
      }

      return this.tableRows.all();
    } catch {
      return [];
    }
  }

  /**
   * Get all event types from the current page
   */
  public async getEventTypes(): Promise<Array<string>> {
    try {
      const rows = await this.getEventLogRows();

      if (rows.length === 0) {
        return [];
      }

      const eventTypes: Array<string> = [];

      for (const row of rows) {
        // Event type is typically in the second column (index 1)
        const cells = row.locator("td");
        const eventCell = cells.nth(1);
        const text = await eventCell.textContent();

        if (text !== null) {
          eventTypes.push(text.trim());
        }
      }

      return eventTypes;
    } catch {
      return [];
    }
  }

  /**
   * Check if next page is available
   */
  public async hasNextPage(): Promise<boolean> {
    try {
      return await this.paginationNextButton.isEnabled();
    } catch {
      return false;
    }
  }

  /**
   * Check if previous page is available
   */
  public async hasPreviousPage(): Promise<boolean> {
    try {
      return await this.paginationPrevButton.isEnabled();
    } catch {
      return false;
    }
  }

  /**
   * Check if audit log table is visible
   */
  public async isAuditLogTableVisible(): Promise<boolean> {
    try {
      await this.eventsTable.waitFor({ state: "visible", timeout: 5000 });

      return true;
    } catch {
      return false;
    }
  }

  /**
   * Navigate to audit log tab for a specific DAG
   */
  public async navigateToAuditLog(dagId: string): Promise<void> {
    await this.navigateTo(EventsPage.getEventsUrl(dagId));
  }

  /**
   * Wait for table to finish loading
   */
  public async waitForTableLoad(): Promise<void> {
    try {
      await this.skeletonLoader.waitFor({ state: "hidden", timeout: 10_000 });
    } catch {
      // Skeleton may not appear if data loads quickly
    }
    await this.eventsTable.waitFor({ state: "visible", timeout: 10_000 });
  }
}
