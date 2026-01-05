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
    // Page URLs
  public static get eventsListUrl(): string {
    return "/events";
  }

  public static get eventsListPaginatedURL(): string {
    return 'events?limit=1&offset=1'
  }

  public readonly eventsPageTitle: Locator;
  public readonly eventsTable: Locator;
  public readonly filterBar: Locator;
  public readonly timestampColumn: Locator;
  public readonly eventTypeColumn: Locator;
  public readonly dagIdColumn: Locator;
  public readonly userColumn: Locator;
  public readonly extraColumn: Locator;
  public readonly runIdColumn: Locator;
  public readonly taskIdColumn: Locator;
  public readonly mapIndexColumn: Locator;
  public readonly tryNumberColumn: Locator;
  public readonly paginationNextButton: Locator;
  public readonly paginationPrevButton: Locator;

  public constructor(page: Page) {
    super(page);
    this.eventsPageTitle = page.locator('h2:has-text("Audit Log")');
    this.eventsTable = page.getByRole("table");
    this.filterBar = page.locator('div:has(button:has-text("Filter"))').first();
    // Use table header selectors - will check for presence of key columns
    this.timestampColumn = page.locator("thead th").first();
    this.eventTypeColumn = page.locator("thead th").nth(1);
    this.userColumn = page.locator("thead th").nth(2);
    this.extraColumn = page.locator("thead th").nth(3);
    this.dagIdColumn = page.locator("thead th").nth(4);
    this.runIdColumn = page.locator("thead th").nth(5);
    this.taskIdColumn = page.locator("thead th").nth(6);
    this.mapIndexColumn = page.locator("thead th").nth(7);
    this.tryNumberColumn = page.locator("thead th").nth(8);
    this.paginationNextButton = page.locator('[data-testid="next"]');
    this.paginationPrevButton = page.locator('[data-testid="prev"]');
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
   * Wait for events table to be visible
   */
  public async waitForEventsTable(): Promise<void> {
    await expect(this.eventsTable).toBeVisible({ timeout: 10_000 });
  }

  /**
   * Get the number of table rows with data
   */
  public async getTableRowCount(): Promise<number> {
    await this.waitForEventsTable();
    return await this.eventsTable.locator("tbody tr").count();
  }

  /**
   * Get text content from a specific table cell
   */
  public async getCellContent(rowIndex: number, columnIndex: number): Promise<string> {
    const cell = this.eventsTable.locator(`tbody tr:nth-child(${rowIndex + 1}) td:nth-child(${columnIndex + 1})`);
    await expect(cell).toBeVisible();
    
    // Try multiple methods to get cell content, as WebKit might behave differently
    let content = await cell.textContent();
    
    if (!content || content.trim() === "") {
      // Fallback: try innerText
      content = await cell.innerText().catch(() => "");
    }
    
    if (!content || content.trim() === "") {
      // Fallback: try getting content from child elements
      const childContent = await cell.locator("*").first().textContent().catch(() => "");
      content = childContent;
    }
    
    return content || "";
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

   public async waitForTimeout(timeoutMs: number = 2000): Promise<void> {
     await this.page.waitForTimeout(timeoutMs);
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
         const firstRow = document.querySelector('table tbody tr');
         return firstRow && firstRow.textContent !== beforeContent;
       },
       firstRowBefore,
       { timeout: 10000 }
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
    * Click on a column header to sort
    */
   public async clickColumnHeader(columnIndex: number): Promise<void> {
     const header = this.eventsTable.locator(`thead th:nth-child(${columnIndex + 1})`);
     await header.click();
     await this.waitForEventsTable();
   }

   /**
    * Get sort indicator from column header
    */
   public async getColumnSortIndicator(columnIndex: number): Promise<string> {
     const header = this.eventsTable.locator(`thead th:nth-child(${columnIndex + 1})`);
     
     // Check for SVG elements with sort-related aria-label
     const sortSvg = header.locator('svg[aria-label*="sorted"]');
     const svgCount = await sortSvg.count();
     
     if (svgCount > 0) {
       const svgAriaLabel = await sortSvg.first().getAttribute('aria-label');
       
       if (svgAriaLabel) {
         if (svgAriaLabel.includes('ascending')) {
           return 'ascending';
         }
         if (svgAriaLabel.includes('descending')) {
           return 'descending';
         }
       }
     }
     
     return 'none';
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
}