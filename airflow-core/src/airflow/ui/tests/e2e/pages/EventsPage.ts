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

/**
 * Events Page Object
 */
export class EventsPage extends BasePage {
  public static get eventsListPaginatedURL(): string {
    return "events?limit=5&offset=1";
  }

  // Page URLs
  public static get eventsListUrl(): string {
    return "/events";
  }

  public readonly dagIdColumn: Locator;
  public readonly eventsPageTitle: Locator;
  public readonly eventsTable: Locator;
  public readonly eventTypeColumn: Locator;
  public readonly extraColumn: Locator;
  public readonly filterBar: Locator;
  public readonly mapIndexColumn: Locator;
  public readonly paginationNextButton: Locator;
  public readonly paginationPrevButton: Locator;
  public readonly runIdColumn: Locator;
  public readonly taskIdColumn: Locator;
  public readonly timestampColumn: Locator;
  public readonly tryNumberColumn: Locator;
  public readonly userColumn: Locator;

  public constructor(page: Page) {
    super(page);
    this.eventsPageTitle = page.locator('h2:has-text("Audit Log")');
    this.eventsTable = page.getByRole("table");
    this.filterBar = page.locator('div:has(button:has-text("Filter"))').first();
    this.timestampColumn = page.getByTestId("table-list").locator('th:has-text("when")');
    this.eventTypeColumn = page.getByTestId("table-list").locator('th:has-text("event")');
    this.userColumn = page.getByTestId("table-list").locator('th:has-text("user")');
    this.extraColumn = page.getByTestId("table-list").locator('th:has-text("extra")');
    this.dagIdColumn = page.getByTestId("table-list").locator('th:has-text("dag id")');
    this.runIdColumn = page.getByTestId("table-list").locator('th:has-text("run id")');
    this.taskIdColumn = page.getByTestId("table-list").locator('th:has-text("task id")');
    this.mapIndexColumn = page.getByTestId("table-list").locator('th:has-text("map index")');
    this.tryNumberColumn = page.getByTestId("table-list").locator('th:has-text("try number")');
    this.paginationNextButton = page.locator('[data-testid="next"]');
    this.paginationPrevButton = page.locator('[data-testid="prev"]');
  }

  /**
   * Click on a column header to sort by column name
   */
  public async clickColumnHeader(columnName: string): Promise<void> {
    const header = this.getColumnLocator(columnName);

    await header.click();
    await this.waitForEventsTable();
  }

  /**
   * Click next page button
   */
  public async clickNextPage(): Promise<void> {
    await expect(this.paginationNextButton).toBeEnabled();

    // Get first row content before click to detect change
    const firstRowBefore = await this.eventsTable.locator("tbody tr").first().textContent();

    await this.paginationNextButton.click();

    // Wait for table content to change (indicating pagination happened)
    await this.page.waitForFunction(
      (beforeContent) => {
        const firstRow = document.querySelector("table tbody tr");

        return firstRow && firstRow.textContent !== beforeContent;
      },
      firstRowBefore,
      { timeout: 10_000 },
    );

    await this.waitForEventsTable();
  }

  /**
   * Click previous page button
   */
  public async clickPrevPage(): Promise<void> {
    await this.paginationPrevButton.click();
    await this.waitForEventsTable();
  }

  /**
   * Get text content from a specific table cell
   */
  public async getCellContent(rowIndex: number, columnIndex: number): Promise<string> {
    const cell = this.eventsTable.locator(
      `tbody tr:nth-child(${rowIndex + 1}) td:nth-child(${columnIndex + 1})`,
    );

    await expect(cell).toBeVisible();

    let content = (await cell.textContent()) ?? "";

    if (content.trim() === "") {
      // Fallback: try getting content from child elements
      const childContent = await cell
        .locator("*")
        .first()
        .textContent()
        .catch(() => "");

      content = childContent ?? "";
    }

    return content;
  }

  /**
   * Get sort indicator from column header by column name
   */
  public async getColumnSortIndicator(columnName: string): Promise<string> {
    const header = this.getColumnLocator(columnName);

    // Check for SVG elements with sort-related aria-label
    const sortSvg = header.locator('svg[aria-label*="sorted"]');
    const svgCount = await sortSvg.count();

    if (svgCount > 0) {
      const ariaLabel = (await sortSvg.first().getAttribute("aria-label")) ?? "";

      if (ariaLabel) {
        if (ariaLabel.includes("sorted-ascending") || ariaLabel.includes("ascending")) {
          return "ascending";
        }
        if (ariaLabel.includes("sorted-descending") || ariaLabel.includes("descending")) {
          return "descending";
        }
      }
    }

    return "none";
  }

  /**
   * Get the number of table rows with data
   */
  public async getTableRowCount(): Promise<number> {
    await this.waitForEventsTable();

    return await this.eventsTable.locator("tbody tr").count();
  }

  /**
   * Navigate to Events page
   */
  public async navigate(): Promise<void> {
    await this.navigateTo(EventsPage.eventsListUrl);
  }

  public async navigateToPaginatedEventsPage(): Promise<void> {
    await this.navigateTo(EventsPage.eventsListPaginatedURL);
  }

  /**
   * Verify that log entries contain expected data patterns
   */
  public async verifyLogEntriesWithData(): Promise<void> {
    const rowCount = await this.getTableRowCount();

    expect(rowCount).toBeGreaterThan(1);

    // Check event column (second column) - should contain event type
    const eventText = await this.getCellContent(0, 1);

    expect(eventText.trim()).not.toBe("");

    // Check user column (third column) - should contain user info
    const userText = await this.getCellContent(0, 2);

    expect(userText.trim()).not.toBe("");
  }

  /**
   * Verify that required table columns are visible
   */
  public async verifyTableColumns(): Promise<void> {
    // Wait for table headers to be present
    await expect(this.eventsTable.locator("thead")).toBeVisible();

    // Verify we have table headers (at least 4 core columns)
    const headerCount = await this.eventsTable.locator("thead th").count();

    expect(headerCount).toBeGreaterThanOrEqual(4);

    // Verify the first few essential columns are present
    await expect(this.timestampColumn).toBeVisible();
    await expect(this.eventTypeColumn).toBeVisible();
    await expect(this.dagIdColumn).toBeVisible();
    await expect(this.userColumn).toBeVisible();
    await expect(this.extraColumn).toBeVisible();
    await expect(this.runIdColumn).toBeVisible();
    await expect(this.taskIdColumn).toBeVisible();
    await expect(this.mapIndexColumn).toBeVisible();
    await expect(this.tryNumberColumn).toBeVisible();
  }

  /**
   * Wait for events table to be visible
   */
  public async waitForEventsTable(): Promise<void> {
    await expect(this.eventsTable).toBeVisible({ timeout: 10_000 });
  }

  public async waitForTimeout(timeoutMs: number = 2000): Promise<void> {
    await this.page.waitForTimeout(timeoutMs);
  }

  /**
   * Get column locator by column name
   */
  private getColumnLocator(columnName: string): Locator {
    const columnMap: Record<string, Locator> = {
      "dag id": this.dagIdColumn,
      event: this.eventTypeColumn,
      extra: this.extraColumn,
      "map index": this.mapIndexColumn,
      "run id": this.runIdColumn,
      "task id": this.taskIdColumn,
      "try number": this.tryNumberColumn,
      user: this.userColumn,
      when: this.timestampColumn,
    };

    const header = columnMap[columnName.toLowerCase()];

    if (!header) {
      throw new Error(`Column "${columnName}" not found in column map`);
    }

    return header;
  }
}
