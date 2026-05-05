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
  public static get connectionsListUrl(): string {
    return "/connections";
  }

  public readonly addButton: Locator;
  public readonly connectionForm: Locator;
  public readonly connectionIdHeader: Locator;
  public readonly connectionIdInput: Locator;
  public readonly connectionRows: Locator;
  // Core page elements
  public readonly connectionsTable: Locator;
  public readonly connectionTypeHeader: Locator;
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
  public readonly tableHeader: Locator;
  public readonly testConnectionButton: Locator;

  public constructor(page: Page) {
    super(page);
    this.connectionsTable = page.getByRole("grid").or(page.locator("table"));
    this.emptyState = page.getByText(/no connection found/i);

    this.addButton = page.getByRole("button", { name: "Add Connection" });
    this.testConnectionButton = page.getByRole("button", { name: "Test" });
    this.saveButton = page.getByRole("button", { name: /^save$/i });

    // Scoped via input[name] because Chakra UI forms may lack
    // associated <label> elements, making getByLabel unreliable.
    this.connectionForm = page.getByRole("dialog");
    this.connectionIdInput = page.locator('input[name="connection_id"]').first();
    this.hostInput = page.locator('input[name="host"]').first();
    this.portInput = page.locator('input[name="port"]').first();
    this.loginInput = page.locator('input[name="login"]').first();
    this.passwordInput = page.locator('input[name="password"]').first();
    this.schemaInput = page.locator('input[name="schema"]').first();
    this.descriptionInput = page.locator('[name="description"]').first();
    // Alerts
    this.successAlert = page.locator('[data-scope="toast"][data-part="root"]');

    this.rowsPerPageSelect = page.locator("select");

    // Sorting and filtering
    this.tableHeader = this.connectionsTable.getByRole("columnheader").first();

    this.connectionIdHeader = this.connectionsTable
      .getByRole("columnheader")
      .filter({ hasText: "Connection ID" });
    this.connectionTypeHeader = this.connectionsTable
      .getByRole("columnheader")
      .filter({ hasText: "Connection Type" });
    this.hostHeader = this.connectionsTable.getByRole("columnheader").filter({ hasText: "Host" });

    this.searchInput = page.getByPlaceholder(/search/i).first();
    // All table body rows (used by connectionRows for web-first assertions)
    this.connectionRows = page.locator("tbody tr");
  }

  public async clickAddButton(): Promise<void> {
    await expect(this.addButton).toBeVisible({ timeout: 5000 });
    await expect(this.addButton).toBeEnabled({ timeout: 5000 });
    await this.addButton.click();
    // Wait for form to load
    await expect(this.connectionIdInput).toBeVisible({ timeout: 10_000 });
  }

  public async clickEditButton(connectionId: string): Promise<void> {
    const row = await this.findConnectionRow(connectionId);

    if (!row) {
      throw new Error(`Connection ${connectionId} not found`);
    }

    const editButton = row.getByRole("button", { name: "Edit Connection" });

    await expect(editButton).toBeVisible({ timeout: 5000 });
    await expect(editButton).toBeEnabled({ timeout: 5000 });
    await editButton.click();
    await expect(this.connectionForm).toBeVisible({ timeout: 10_000 });
    await expect(this.connectionIdInput).toBeVisible({ timeout: 10_000 });
  }
  // Create a new connection with full workflow
  public async createConnection(details: ConnectionDetails): Promise<void> {
    await this.clickAddButton();
    await this.fillConnectionForm(details);
    await this.saveConnection();
    await this.waitForConnectionsListLoad();
  }

  public async deleteConnection(connectionId: string): Promise<void> {
    const row = await this.findConnectionRow(connectionId);

    if (!row) {
      throw new Error(`Connection ${connectionId} not found`);
    }

    const deleteButton = row.getByRole("button", { name: "Delete Connection" });

    await expect(deleteButton).toBeVisible({ timeout: 10_000 });
    await expect(deleteButton).toBeEnabled({ timeout: 5000 });
    await deleteButton.click();

    // Wait for the dialog to finish its open animation (data-state="open" is set by
    // Ark UI once the transition completes). Without this, the backdrop can cover the
    // confirm button during the animation and cause the click to time out.
    const deleteDialog = this.page.locator('[role="dialog"][data-state="open"]');

    await deleteDialog.waitFor({ state: "visible", timeout: 10_000 });
    const confirmButton = deleteDialog.getByRole("button", { name: "Yes, Delete" });

    await expect(confirmButton).toBeVisible({ timeout: 5000 });

    await expect(confirmButton).toBeEnabled({ timeout: 5000 });
    await confirmButton.click();

    await expect(this.getConnectionRow(connectionId)).not.toBeVisible({ timeout: 15_000 });
  }

  public async editConnection(connectionId: string, updates: Partial<ConnectionDetails>): Promise<void> {
    await this.clickEditButton(connectionId);

    await expect(this.connectionIdInput).toBeVisible({ timeout: 10_000 });
    await expect(this.connectionIdInput).toBeEnabled({ timeout: 10_000 });
    await expect(this.connectionIdInput).toHaveValue(connectionId, { timeout: 10_000 });

    // Fill the fields that need updating
    await this.fillConnectionForm(updates);
    await this.saveConnection();
  }

  public async fillConnectionForm(details: Partial<ConnectionDetails>): Promise<void> {
    if (details.connection_id !== undefined && details.connection_id !== "") {
      await this.connectionIdInput.fill(details.connection_id);
    }

    if (details.conn_type !== undefined && details.conn_type !== "") {
      // Scope the combobox to the form dialog to avoid matching stale elements
      // outside the form. Wait for it to become actionable before opening the
      // list, which avoids races when the dialog is still settling.
      const selectCombobox = this.connectionForm.getByRole("combobox").first();
      const option = this.page.getByRole("option", { name: new RegExp(details.conn_type, "i") }).first();

      await expect(async () => {
        await expect(selectCombobox).toBeVisible({ timeout: 10_000 });
        await expect(selectCombobox).toBeEnabled({ timeout: 10_000 });
        await selectCombobox.click({ timeout: 5000 });
        await expect(option).toBeVisible({ timeout: 10_000 });
      }).toPass({ intervals: [1000, 2000, 3000], timeout: 30_000 });

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

        await monacoEditor.waitFor({ state: "visible", timeout: 30_000 });

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

        await this.connectionIdInput.click();
      }
    }
  }

  public async getConnectionCount(): Promise<number> {
    const ids = await this.getConnectionIds();

    return ids.length;
  }

  public async getConnectionIds(): Promise<Array<string>> {
    const rowLocator = this.connectionRows;
    const countRow = await rowLocator.count();

    if (countRow === 0) {
      return [];
    }

    await expect(rowLocator.first()).toBeVisible({ timeout: 5000 });

    let stableRowCount = 0;
    let lastSeenCount = -1;

    await expect
      .poll(
        async () => {
          const count = await this.page.locator("tbody tr").count();

          if (count > 0 && count === lastSeenCount) {
            stableRowCount = count;

            return true;
          }
          lastSeenCount = count;
          await this.page.waitForTimeout(200);

          return false;
        },
        { intervals: [100, 200, 500, 500], timeout: 10_000 },
      )
      .toBeTruthy()
      .catch(() => {
        // If timeout, fall back to last observed count
        stableRowCount = lastSeenCount > 0 ? lastSeenCount : 0;
      });

    if (stableRowCount === 0) {
      return [];
    }

    const connectionIds: Array<string> = [];

    for (let i = 0; i < stableRowCount; i++) {
      try {
        const row = rowLocator.nth(i);
        const cells = row.locator("td");
        const cellCount = await cells.count();

        if (cellCount > 1) {
          // Second cell (after checkbox) contains the connection ID.
          const idCell = cells.nth(1);
          const text = await idCell.textContent({ timeout: 3000 });

          if (text !== null && text.trim() !== "") {
            connectionIds.push(text.trim());
          }
        }
      } catch {
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
    await expect(async () => {
      await this.navigateTo(ConnectionsPage.connectionsListUrl);
      await this.waitForConnectionsListLoad();
    }).toPass({ intervals: [2000], timeout: 60_000 });
  }

  public async saveConnection(): Promise<void> {
    await expect(this.saveButton).toBeVisible({ timeout: 10_000 });
    await expect(this.saveButton).toBeEnabled({ timeout: 5000 });

    const responsePromise = this.page.waitForResponse(
      (response) =>
        /\/api\/v2\/connections(\/[^/]+)?(\?|$)/.test(response.url()) &&
        ["PATCH", "POST"].includes(response.request().method()) &&
        response.ok(),
      { timeout: 15_000 },
    );

    await this.saveButton.click();
    await responsePromise;
  }

  public async searchConnections(searchTerm: string): Promise<void> {
    await this.searchInput.fill(searchTerm);

    if (searchTerm === "") {
      await expect(this.connectionRows.first().or(this.emptyState)).toBeVisible({ timeout: 10_000 });
    } else {
      // Wait for a matching row or the empty state to appear — this directly checks
      // what the user sees and avoids a race where an empty loading state satisfies
      // "no non-matching rows" before results arrive.
      const matchingRow = this.page.locator("tbody tr").filter({ hasText: searchTerm });

      await expect(matchingRow.first().or(this.emptyState)).toBeVisible({ timeout: 10_000 });
    }
  }

  public async verifyConnectionInList(connectionId: string, expectedType: string): Promise<void> {
    const row = await this.findConnectionRow(connectionId);

    if (!row) {
      throw new Error(`Connection ${connectionId} not found in list`);
    }

    await expect(row).toContainText(connectionId);
    await expect(row).toContainText(expectedType);
  }

  private async findConnectionRow(connectionId: string): Promise<Locator | undefined> {
    // Try search first (faster)
    const hasSearch = await this.searchInput
      .waitFor({ state: "visible", timeout: 3000 })
      .then(() => true)
      .catch(() => false);

    if (hasSearch) {
      return await this.findConnectionRowUsingSearch(connectionId);
    }

    return undefined;
  }

  private async findConnectionRowUsingSearch(connectionId: string): Promise<Locator | undefined> {
    await this.waitForConnectionsListLoad();
    await this.searchConnections(connectionId);

    const isTableVisible = await this.connectionsTable.isVisible({ timeout: 5000 }).catch(() => false);

    if (!isTableVisible) {
      return undefined;
    }

    const row = this.page.locator("tbody tr").filter({ hasText: connectionId }).first();

    // Use web-first assertion (toBeVisible) rather than manual isVisible() check
    try {
      await expect(row).toBeVisible({ timeout: 10_000 });
    } catch {
      return undefined;
    }

    return row;
  }

  private async waitForConnectionsListLoad(): Promise<void> {
    await expect(this.page).toHaveURL(/\/connections/, { timeout: 10_000 });
    await this.page.waitForLoadState("domcontentloaded");

    const table = this.connectionsTable;

    await expect(table.or(this.emptyState)).toBeVisible({ timeout: 10_000 });

    if (await table.isVisible().catch(() => false)) {
      await expect(this.connectionRows.first()).toBeVisible({
        timeout: 10_000,
      });
    }
  }
}
