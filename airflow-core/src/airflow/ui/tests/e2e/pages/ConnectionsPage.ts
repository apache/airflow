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

type ConnectionDetails = {
  conn_type: string;
  connection_id: string;
  description?: string;
  extra?: string;
  host?: string;
  login?: string;
  password?: string;
  port?: number | string;
  schema?: string;
};

export class ConnectionsPage extends BasePage {
  // Page URLs
  public static get connectionsListUrl(): string {
    return "/connections";
  }

  public readonly addButton: Locator;
  public readonly cancelDeleteButton: Locator;
  public readonly confirmDeleteButton: Locator;
  public readonly connectionIdHeader: Locator;
  public readonly connectionIdInput: Locator;
  // Core page elements
  public readonly connectionsTable: Locator;
  public readonly connectionTypeHeader: Locator;
  public readonly connectionTypeSelect: Locator;
  public readonly descriptionInput: Locator;
  public readonly emptyState: Locator;
  public readonly hostHeader: Locator;
  public readonly hostInput: Locator;
  public readonly loginInput: Locator;

  // Pagination elements
  public readonly paginationNextButton: Locator;
  public readonly paginationPrevButton: Locator;
  public readonly passwordInput: Locator;

  public readonly portInput: Locator;
  public readonly rowsPerPageSelect: Locator;
  public readonly saveButton: Locator;

  public readonly schemaInput: Locator;
  public readonly searchInput: Locator;
  public readonly successAlert: Locator;
  // Sorting and filtering
  public readonly tableHeader: Locator;
  public readonly testConnectionButton: Locator;

  public constructor(page: Page) {
    super(page);
    // Table elements (Chakra UI DataTable)
    this.connectionsTable = page.locator('[role="grid"], table');
    this.emptyState = page.locator("text=/No connection found!/i");

    // Action buttons
    this.addButton = page.getByRole("button", { name: "Add Connection" });
    this.testConnectionButton = page.locator('button:has-text("Test")');
    this.saveButton = page.getByRole("button", { name: /^save$/i });

    // Form inputs (Chakra UI inputs)
    this.connectionIdInput = page.locator('input[name="connection_id"]').first();
    this.connectionTypeSelect = page.getByRole("combobox").first();
    this.hostInput = page.locator('input[name="host"]').first();
    this.portInput = page.locator('input[name="port"]').first();
    this.loginInput = page.locator('input[name="login"]').first();
    this.passwordInput = page.locator('input[name="password"], input[type="password"]').first();
    this.schemaInput = page.locator('input[name="schema"]').first();
    // Try multiple possible selectors
    this.descriptionInput = page.locator('[name="description"]').first();

    // Alerts
    this.successAlert = page.locator('[data-scope="toast"][data-part="root"]');

    // Delete confirmation dialog
    this.confirmDeleteButton = page.locator('button:has-text("Delete")').first();
    this.cancelDeleteButton = page.locator('button:[aria-label*="Cancel"]').first();

    // Pagination
    this.paginationNextButton = page.locator('[data-testid="next"]');
    this.paginationPrevButton = page.locator('[data-testid="prev"]');
    this.rowsPerPageSelect = page.locator("select");

    // Sorting and filtering
    this.tableHeader = page.locator('[role="columnheader"]').first();
    this.connectionIdHeader = page.locator('[role="columnheader"]:has-text("Connection ID")').first();
    this.connectionTypeHeader = page.locator('[role="columnheader"]:has-text("Connection Type")').first();
    this.hostHeader = page.locator('[role="columnheader"]:has-text("Host")').first();
    this.searchInput = page.locator('input[placeholder*="Search"], input[placeholder*="search"]').first();
  }

  // Click the Add button to create a new connection
  public async clickAddButton(): Promise<void> {
    await expect(this.addButton).toBeVisible({ timeout: 5000 });
    await this.addButton.click();
    // Wait for form to load
    await expect(this.connectionIdInput).toBeVisible({ timeout: 3000 });
  }

  // Click edit button for a specific connection
  public async clickEditButton(connectionId: string): Promise<void> {
    const row = await this.findConnectionRow(connectionId);

    if (!row) {
      throw new Error(`Connection ${connectionId} not found`);
    }

    const editButton = row.getByRole("button", { name: "Edit Connection" });

    await expect(editButton).toBeVisible({ timeout: 2000 });
    await editButton.click();
    await expect(this.connectionIdInput).toBeVisible({ timeout: 3000 });
  }

  public async clickNextPage(): Promise<void> {
    const isEnabled = await this.paginationNextButton.isEnabled({ timeout: 2000 }).catch(() => false);

    if (!isEnabled) {
      return;
    }

    await expect(this.paginationNextButton).toBeVisible({ timeout: 5000 });
    await this.paginationNextButton.click();

    await this.waitForConnectionsListLoad();
  }

  // Click next page button for pagination
  public async clickPrevPage(): Promise<void> {
    const isEnabled = await this.paginationPrevButton.isEnabled({ timeout: 2000 }).catch(() => false);

    if (!isEnabled) {
      return;
    }

    await expect(this.paginationPrevButton).toBeVisible({ timeout: 5000 });
    await this.paginationPrevButton.click();

    await this.waitForConnectionsListLoad();
  }

  // Check if a connection exists in the current view
  public async connectionExists(connectionId: string): Promise<boolean> {
    const emptyState = await this.page
      .locator("text=No connection found!")
      .isVisible({ timeout: 1000 })
      .catch(() => false);

    if (emptyState) {
      return false;
    }
    const row = await this.findConnectionRow(connectionId);
    const visible = row !== null;

    return visible;
  }

  // Create a new connection with full workflow
  public async createConnection(details: ConnectionDetails): Promise<void> {
    await this.clickAddButton();
    await this.fillConnectionForm(details);
    await this.saveConnection();
  }

  // Delete a connection by connection ID
  public async deleteConnection(connectionId: string): Promise<void> {
    // await this.navigate();
    const row = await this.findConnectionRow(connectionId);

    if (!row) {
      throw new Error(`Connection ${connectionId} not found`);
    }

    // Find delete button in the row
    await this.page.evaluate(() => {
      const backdrops = document.querySelectorAll<HTMLElement>('[data-scope="dialog"][data-part="backdrop"]');

      backdrops.forEach((backdrop) => {
        const { state } = backdrop.dataset;

        if (state === "closed") {
          backdrop.remove();
        }
      });
    });
    const deleteButton = row.getByRole("button", { name: "Delete Connection" });

    await expect(deleteButton).toBeVisible({ timeout: 1000 });
    await deleteButton.click();

    // Handle delete confirmation - try multiple button texts
    await expect(this.confirmDeleteButton).toBeVisible({ timeout: 5000 });
    await this.confirmDeleteButton.click();

    await expect(this.emptyState).toBeVisible({ timeout: 5000 });
    await this.searchInput.clear();
  }

  // Edit a connection by connection ID
  public async editConnection(connectionId: string, updates: Partial<ConnectionDetails>): Promise<void> {
    const row = await this.findConnectionRow(connectionId);

    if (!row) {
      throw new Error(`Connection ${connectionId} not found`);
    }

    await this.clickEditButton(connectionId);

    // Wait for form to load
    await expect(this.connectionIdInput).toBeVisible({ timeout: 3000 });

    // Fill the fields that need updating
    await this.fillConnectionForm(updates);
    await this.saveConnection();
    await this.searchInput.clear();
  }

  // Fill connection form with details
  public async fillConnectionForm(details: Partial<ConnectionDetails>): Promise<void> {
    if (details.connection_id !== undefined && details.connection_id !== "") {
      await this.connectionIdInput.fill(details.connection_id);
    }

    if (details.conn_type !== undefined && details.conn_type !== "") {
      // Click the select field to open the dropdown
      const selectCombobox = this.page.getByRole("combobox").first();

      await selectCombobox.click({ timeout: 3000 }).catch(() => {
        // Dropdown might already be open
      });

      // Wait for options to appear and click the matching option
      const option = this.page.getByRole("option", { name: new RegExp(details.conn_type, "i") }).first();

      await option.click({ timeout: 2000 }).catch(() => {
        // If option click fails, try typing in the input
        if (details.conn_type !== undefined && details.conn_type !== "") {
          void this.page.keyboard.type(details.conn_type);
        }
      });
    }

    if (details.host !== undefined && details.host !== "") {
      await expect(this.hostInput).toBeVisible({ timeout: 2000 });
      await this.hostInput.fill(details.host);
    }

    if (details.port !== undefined && details.port !== "") {
      await expect(this.portInput).toBeVisible({ timeout: 2000 });
      await this.portInput.fill(String(details.port));
    }

    if (details.login !== undefined && details.login !== "") {
      await expect(this.loginInput).toBeVisible({ timeout: 2000 });
      await this.loginInput.fill(details.login);
    }

    if (details.password !== undefined && details.password !== "") {
      await expect(this.passwordInput).toBeVisible({ timeout: 2000 });
      await this.passwordInput.fill(details.password);
    }

    if (details.description !== undefined && details.description !== "") {
      await expect(this.descriptionInput).toBeVisible({ timeout: 2000 });
      await this.descriptionInput.fill(details.description);
    }

    if (details.schema !== undefined && details.schema !== "") {
      await expect(this.schemaInput).toBeVisible({ timeout: 2000 });
      await this.schemaInput.fill(details.schema);
    }

    if (details.extra !== undefined && details.extra !== "") {
      const extraAccordion = this.page.locator('button:has-text("Extra Fields JSON")').first();
      const accordionVisible = await extraAccordion.isVisible({ timeout: 3000 }).catch(() => false);

      if (accordionVisible) {
        await extraAccordion.click();
        const extraEditor = this.page.locator('.cm-content[contenteditable="true"]:visible').first();

        await extraEditor.waitFor({ state: "visible", timeout: 3000 });
        await extraEditor.clear();
        await extraEditor.fill(details.extra);
        await extraEditor.blur();
      }
    }
  }

  // Get the current sort order of a column
  public async getColumnSortOrder(headerName: string): Promise<"asc" | "desc" | null> {
    const header = this.page.locator(`th:has-text("${headerName}")`).first();
    const sortIcon = header.locator('[aria-label*="sort"], [data-testid*="sort"]');

    if (await sortIcon.isVisible()) {
      const ariaSort = await header.getAttribute("aria-sort");

      if (ariaSort === "ascending") return "asc";
      if (ariaSort === "descending") return "desc";
    }

    return null;
  }

  // Get connection count from current page
  public async getConnectionCount(): Promise<number> {
    const ids = await this.getConnectionIds();

    return ids.length;
  }

  // Get all connection IDs from the current page
  public async getConnectionIds(): Promise<Array<string>> {
    await expect(this.page.locator("tbody tr").first()).toBeVisible({ timeout: 5000 });

    let rows = this.page.locator("tbody tr");
    let rowCount = await rows.count();

    const connectionIds: Array<string> = [];

    // Process all rows
    for (let i = 0; i < rowCount; i++) {
      try {
        const row = rows.nth(i);
        const cells = row.locator("td");
        const cellCount = await cells.count();

        if (cellCount > 1) {
          // Connection ID is typically in the second cell (after checkbox)
          const idCell = cells.nth(1);
          const text = await idCell.textContent({ timeout: 3000 });

          if (text !== null && text.trim() !== "") {
            connectionIds.push(text.trim());
          }
        }
      } catch {
        // Skip rows that can't be read
        continue;
      }
    }

    return connectionIds;
  }

  // Check if pagination is visible
  public async isPaginationVisible(): Promise<boolean> {
    try {
      const nextVisible = await this.paginationNextButton.isVisible({ timeout: 2000 });
      const prevVisible = await this.paginationPrevButton.isVisible({ timeout: 2000 });

      return nextVisible || prevVisible;
    } catch {
      return false;
    }
  }

  // Navigate to Connections list page
  public async navigate(): Promise<void> {
    await this.navigateTo(ConnectionsPage.connectionsListUrl);
    await this.waitForConnectionsListLoad();
  }

  // Save the connection form
  public async saveConnection(): Promise<void> {
    await this.saveButton.scrollIntoViewIfNeeded();
    await expect(this.saveButton).toBeVisible({ timeout: 2000 });
    await this.saveButton.click();

    // Wait for either redirect OR success message
    await Promise.race([
      this.page.waitForURL("**/connections", { timeout: 10_000 }),
      this.successAlert.waitFor({ state: "visible", timeout: 10_000 }),
    ]);
  }

  // Search for connections using the search input
  public async searchConnections(searchTerm: string): Promise<void> {
    if (await this.searchInput.isVisible()) {
      await this.searchInput.fill(searchTerm);
    }
  }

  // Sort connections by clicking header
  public async sortByHeader(headerName: string): Promise<void> {
    const header = this.page.locator(`th:has-text("${headerName}")`).first();

    await expect(header).toBeVisible({ timeout: 5000 });
    await header.click();
    // Wait for sort to apply
    await this.page.waitForLoadState("networkidle", { timeout: 3000 });
  }

  // Verify connection details are displayed in the list
  public async verifyConnectionInList(connectionId: string, expectedType: string): Promise<void> {
    const row = await this.findConnectionRow(connectionId);

    if (!row) {
      throw new Error(`Connection ${connectionId} not found in list`);
    }

    const rowText = await row.textContent();

    expect(rowText).toContain(connectionId);
    expect(rowText).toContain(expectedType);
  }

  private async findConnectionRow(connectionId: string): Promise<Locator | null> {
    // Try search first (faster)
    const hasSearch = await this.searchInput.isVisible({ timeout: 500 }).catch(() => false);

    if (hasSearch) {
      return await this.findConnectionRowUsingSearch(connectionId);
    }

    return null;
  }

  private async findConnectionRowUsingSearch(connectionId: string): Promise<Locator | null> {
    await this.searchConnections(connectionId);

    // Check if table is visible (without throwing)
    const isTableVisible = await this.connectionsTable.isVisible({ timeout: 5000 }).catch(() => false);

    if (!isTableVisible) {
      return null;
    }

    const row = this.page.locator("tbody tr").filter({ hasText: connectionId }).first();

    return row;
  }

  // Wait for connections list to fully load
  private async waitForConnectionsListLoad(): Promise<void> {
    await expect(this.page).toHaveURL(/\/connections/, { timeout: 3000 });
    await this.page.waitForLoadState("domcontentloaded");

    const table = this.page.locator("table");

    // Wait for either state to appear
    await Promise.race([
      table.waitFor({ state: "visible", timeout: 10_000 }),
      this.emptyState.waitFor({ state: "visible", timeout: 10_000 }),
    ]);

    if (await table.isVisible().catch(() => false)) {
      // Wait until rows render (state-based, not time-based)
      await this.page
        .locator("tbody tr")
        .first()
        .waitFor({ state: "visible", timeout: 5000 })
        .catch(() => {
          // No rows found - possibly empty table
        });
    }
  }
}
