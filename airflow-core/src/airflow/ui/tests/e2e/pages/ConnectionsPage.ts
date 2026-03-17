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
  public readonly confirmDeleteButton: Locator;
  public readonly connectionForm: Locator;
  public readonly connectionIdHeader: Locator;
  public readonly connectionIdInput: Locator;
  public readonly connectionRows: Locator;
  // Core page elements
  public readonly connectionsTable: Locator;
  public readonly connectionTypeHeader: Locator;
  public readonly connectionTypeSelect: Locator;
  public readonly descriptionInput: Locator;
  public readonly emptyState: Locator;
  public readonly hostHeader: Locator;
  public readonly hostInput: Locator;
  public readonly loginInput: Locator;

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
    this.testConnectionButton = page.getByRole("button", { name: "Test" });
    this.saveButton = page.getByRole("button", { name: /^save$/i });

    // Form inputs (Chakra UI inputs)
    this.connectionForm = page.locator('[data-scope="dialog"][data-part="content"]');
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
    this.confirmDeleteButton = page.getByRole("button", { name: "Delete" }).first();
    this.rowsPerPageSelect = page.locator("select");

    // Sorting and filtering
    this.tableHeader = page.locator('[role="columnheader"]').first();

    this.connectionIdHeader = page
      .locator('[role="columnheader"]')
      .filter({ hasText: "Connection ID" })
      .first();
    this.connectionTypeHeader = page
      .locator('[role="columnheader"]')
      .filter({ hasText: "Connection Type" })
      .first();
    this.hostHeader = page.locator('[role="columnheader"]').filter({ hasText: "Host" }).first();

    this.searchInput = page.locator('input[placeholder*="Search"], input[placeholder*="search"]').first();
    // All table body rows (used by connectionRows for web-first assertions)
    this.connectionRows = page.locator("tbody tr");
  }

  // Click the Add button to create a new connection
  public async clickAddButton(): Promise<void> {
    await expect(this.addButton).toBeVisible({ timeout: 5000 });
    await expect(this.addButton).toBeEnabled({ timeout: 5000 });
    await this.addButton.click();
    // Wait for form to load
    await expect(this.connectionForm).toBeVisible({ timeout: 10_000 });
  }

  // Click edit button for a specific connection
  public async clickEditButton(connectionId: string): Promise<void> {
    // Wait for any stale dialog backdrop to clear before interacting
    const backdrop = this.page.locator('[data-scope="dialog"][data-part="backdrop"]');

    if (await backdrop.isVisible({ timeout: 1000 }).catch(() => false)) {
      await backdrop.waitFor({ state: "detached", timeout: 5000 });
    }

    const row = await this.findConnectionRow(connectionId);

    if (!row) {
      throw new Error(`Connection ${connectionId} not found`);
    }

    const editButton = row.getByRole("button", { name: "Edit Connection" });

    await expect(editButton).toBeVisible({ timeout: 5000 });
    await expect(editButton).toBeEnabled({ timeout: 5000 });
    await editButton.click();
    await expect(this.connectionForm).toBeVisible({ timeout: 10_000 });
  }
  // Create a new connection with full workflow
  public async createConnection(details: ConnectionDetails): Promise<void> {
    await this.clickAddButton();
    await this.fillConnectionForm(details);
    await this.saveConnection();
    await this.waitForConnectionsListLoad();
  }

  // Delete a connection by connection ID
  public async deleteConnection(connectionId: string): Promise<void> {
    // await this.navigate();
    const row = await this.findConnectionRow(connectionId);

    if (!row) {
      throw new Error(`Connection ${connectionId} not found`);
    }

    const deleteButton = row.getByRole("button", { name: "Delete Connection" });

    await expect(deleteButton).toBeVisible({ timeout: 10_000 });
    await expect(deleteButton).toBeEnabled({ timeout: 5000 });
    await deleteButton.click();

    await expect(this.confirmDeleteButton).toBeVisible({ timeout: 10_000 });
    await expect(this.confirmDeleteButton).toBeEnabled({ timeout: 5000 });
    await this.confirmDeleteButton.click();

    await expect(this.getConnectionRow(connectionId)).not.toBeVisible();
  }

  // Edit a connection by connection ID
  public async editConnection(connectionId: string, updates: Partial<ConnectionDetails>): Promise<void> {
    await this.clickEditButton(connectionId);

    // Wait for form to load
    await expect(this.connectionIdInput).toBeVisible({ timeout: 10_000 });

    // Fill the fields that need updating
    await this.fillConnectionForm(updates);
    await this.saveConnection();
  }

  // Fill connection form with details
  public async fillConnectionForm(details: Partial<ConnectionDetails>): Promise<void> {
    if (details.connection_id !== undefined && details.connection_id !== "") {
      await this.connectionIdInput.fill(details.connection_id);
    }

    if (details.conn_type !== undefined && details.conn_type !== "") {
      // Click the select field to open the dropdown
      const selectCombobox = this.page.getByRole("combobox").first();

      await expect(selectCombobox).toBeEnabled({ timeout: 25_000 });

      await selectCombobox.click();

      // Wait for options to appear and click the matching option
      const option = this.page.getByRole("option", { name: new RegExp(details.conn_type, "i") }).first();

      await expect(option).toBeVisible({ timeout: 10_000 });
      await option.click();
    }

    if (details.host !== undefined && details.host !== "") {
      await expect(this.hostInput).toBeVisible({ timeout: 10_000 });
      await this.hostInput.fill(details.host);
    }

    if (details.port !== undefined && details.port !== "") {
      await expect(this.portInput).toBeVisible({ timeout: 10_000 });
      await this.portInput.fill(String(details.port));
    }

    if (details.login !== undefined && details.login !== "") {
      await expect(this.loginInput).toBeVisible({ timeout: 10_000 });
      await this.loginInput.fill(details.login);
    }

    if (details.password !== undefined && details.password !== "") {
      await expect(this.passwordInput).toBeVisible({ timeout: 10_000 });
      await this.passwordInput.fill(details.password);
    }

    if (details.description !== undefined && details.description !== "") {
      await expect(this.descriptionInput).toBeVisible({ timeout: 10_000 });
      await this.descriptionInput.fill(details.description);
    }

    if (details.schema !== undefined && details.schema !== "") {
      await expect(this.schemaInput).toBeVisible({ timeout: 10_000 });
      await this.schemaInput.fill(details.schema);
    }

    if (details.extra !== undefined && details.extra !== "") {
      const extraAccordion = this.page.getByRole("button", { name: "Extra Fields JSON" }).first();
      const accordionVisible = await extraAccordion.isVisible({ timeout: 5000 }).catch(() => false);

      if (accordionVisible) {
        await extraAccordion.click();
        const monacoEditor = this.page.locator(".monaco-editor").first();

        await monacoEditor.waitFor({ state: "visible", timeout: 5000 });

        // Set value via Monaco API to avoid auto-closing bracket/quote issues with keyboard input
        await monacoEditor.evaluate((container, value) => {
          const monacoApi = (globalThis as Record<string, unknown>).monaco as
            | {
                editor: {
                  getEditors: () => Array<{ getDomNode: () => Node; setValue: (v: string) => void }>;
                };
              }
            | undefined;

          if (monacoApi !== undefined) {
            const editor = monacoApi.editor.getEditors().find((e) => container.contains(e.getDomNode()));

            if (editor !== undefined) {
              editor.setValue(value);
            }
          }
        }, details.extra);

        // Click outside to trigger blur
        await this.connectionIdInput.click();
      }
    }
  }

  // Get connection count from current page
  public async getConnectionCount(): Promise<number> {
    const ids = await this.getConnectionIds();

    return ids.length;
  }

  // Get all connection IDs from the current page
  public async getConnectionIds(): Promise<Array<string>> {
    await expect(this.page.locator("tbody tr").first()).toBeVisible({ timeout: 5000 });

    let stableRowCount = 0;

    await expect
      .poll(
        async () => {
          const count = await this.page.locator("tbody tr").count();

          if (count > 0) {
            stableRowCount = count;

            return true;
          }

          return false;
        },
        { intervals: [100, 200, 500], timeout: 10_000 },
      )
      .toBeTruthy()
      .catch(() => {
        // If timeout, just use current count
        stableRowCount = 0;
      });

    if (stableRowCount === 0) {
      return [];
    }

    let rows = this.page.locator("tbody tr");
    const connectionIds: Array<string> = [];

    // Process all rows
    for (let i = 0; i < stableRowCount; i++) {
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

  // Returns a locator for a specific connection row (for web-first assertions in specs)
  public getConnectionRow(connectionId: string): Locator {
    return this.page.locator("tbody tr").filter({ hasText: connectionId }).first();
  }

  // Navigate to Connections list page
  public async navigate(): Promise<void> {
    await this.navigateTo(ConnectionsPage.connectionsListUrl);
    await this.waitForConnectionsListLoad();
  }

  // Save the connection form
  public async saveConnection(): Promise<void> {
    await expect(this.saveButton).toBeVisible({ timeout: 10_000 });
    await expect(this.saveButton).toBeEnabled({ timeout: 5000 });
    await this.saveButton.click();

    // Wait for either redirect OR success message
    await Promise.race([
      this.page.waitForURL("**/connections", { timeout: 10_000 }),
      this.successAlert.waitFor({ state: "visible", timeout: 10_000 }),
    ]);
  }

  public async searchConnections(searchTerm: string): Promise<void> {
    await this.searchInput.fill(searchTerm);

    if (searchTerm === "") {
      await expect(this.connectionRows.first().or(this.emptyState)).toBeVisible({ timeout: 10_000 });
    } else {
      const nonMatchingRow = this.page.locator("tbody tr").filter({ hasNotText: searchTerm });

      await expect(nonMatchingRow).toHaveCount(0, { timeout: 10_000 });

      await expect(this.connectionRows.first()).toBeVisible();
    }
  }

  // Verify connection details are displayed in the list
  public async verifyConnectionInList(connectionId: string, expectedType: string): Promise<void> {
    const row = await this.findConnectionRow(connectionId);

    if (!row) {
      throw new Error(`Connection ${connectionId} not found in list`);
    }

    await expect(row).toContainText(connectionId);
    await expect(row).toContainText(expectedType);
  }

  private async findConnectionRow(connectionId: string): Promise<Locator | null> {
    // Try search first (faster)
    const hasSearch = await this.searchInput.isVisible({ timeout: 3000 }).catch(() => false);

    if (hasSearch) {
      return await this.findConnectionRowUsingSearch(connectionId);
    }

    return null;
  }

  private async findConnectionRowUsingSearch(connectionId: string): Promise<Locator | null> {
    await this.waitForConnectionsListLoad();
    await this.searchConnections(connectionId);

    // Check if table is visible (without throwing)
    const isTableVisible = await this.connectionsTable.isVisible({ timeout: 5000 }).catch(() => false);

    if (!isTableVisible) {
      return null;
    }

    const row = this.page.locator("tbody tr").filter({ hasText: connectionId }).first();

    const rowExists = await row.isVisible({ timeout: 3000 }).catch(() => false);

    if (!rowExists) {
      return null;
    }

    return row;
  }

  // Wait for connections list to fully load
  private async waitForConnectionsListLoad(): Promise<void> {
    await expect(this.page).toHaveURL(/\/connections/, { timeout: 3000 });
    await this.page.waitForLoadState("domcontentloaded");

    const table = this.connectionsTable;

    // Wait for either table or empty state
    await expect(table.or(this.emptyState)).toBeVisible({ timeout: 10_000 });

    if (await table.isVisible().catch(() => false)) {
      await expect(this.connectionRows.first()).toBeVisible({
        timeout: 10_000,
      });
    }
  }
}
