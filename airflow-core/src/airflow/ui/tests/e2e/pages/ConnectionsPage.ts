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
import { waitForStableRowCount } from "tests/e2e/utils/test-helpers";

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
  public readonly confirmDeleteButton: Locator;
  public readonly connectionForm: Locator;
  public readonly connectionIdHeader: Locator;
  public readonly connectionIdInput: Locator;
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
    this.testConnectionButton = page.getByRole("button", { name: /^test$/i });
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

    // Alerts — scoped to the notification region to avoid Monaco editor's role="alert" elements
    this.successAlert = page.getByRole("region", { name: /notifications/i }).getByRole("status");

    // Button text is "Yes, Delete" (not just "Delete")
    this.confirmDeleteButton = page.getByRole("button", { name: /yes, delete/i });
    this.rowsPerPageSelect = page.locator("select");

    // columnheader accessible name is "sort" (from inner button), so filter by text instead.
    this.tableHeader = page.getByRole("columnheader").first();
    this.connectionIdHeader = page.getByRole("columnheader").filter({ hasText: "Connection ID" });
    this.connectionTypeHeader = page.getByRole("columnheader").filter({ hasText: "Connection Type" });
    this.hostHeader = page.getByRole("columnheader").filter({ hasText: /^Host$/ });
    this.searchInput = page.getByPlaceholder(/search/i);
  }

  public async clickAddButton(): Promise<void> {
    await expect(this.addButton).toBeVisible({ timeout: 5000 });
    await expect(this.addButton).toBeEnabled({ timeout: 5000 });
    await this.addButton.click();
    await expect(this.connectionForm).toBeVisible();
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
    await expect(this.connectionForm).toBeVisible();
  }

  public async connectionExists(connectionId: string): Promise<boolean> {
    const emptyState = await this.emptyState.isVisible({ timeout: 1000 }).catch(() => false);

    if (emptyState) {
      return false;
    }

    return (await this.findConnectionRow(connectionId)) !== undefined;
  }

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

    await expect(deleteButton).toBeVisible();
    await expect(deleteButton).toBeEnabled({ timeout: 5000 });

    // Extended timeout: on resource-constrained CI runners, WebKit can take
    // longer than the default 10s to release the previous dialog's backdrop
    // pointer-events after close.
    await deleteButton.click({ timeout: 30_000 });

    await expect(this.confirmDeleteButton).toBeVisible();
    await expect(this.confirmDeleteButton).toBeEnabled({ timeout: 5000 });
    await this.confirmDeleteButton.click();

    await expect(this.emptyState).toBeVisible({ timeout: 5000 });
  }

  public async editConnection(connectionId: string, updates: Partial<ConnectionDetails>): Promise<void> {
    const row = await this.findConnectionRow(connectionId);

    if (!row) {
      throw new Error(`Connection ${connectionId} not found`);
    }

    await this.clickEditButton(connectionId);
    await expect(this.connectionIdInput).toBeVisible();
    await this.fillConnectionForm(updates);
    await this.saveConnection();
  }

  public async fillConnectionForm(details: Partial<ConnectionDetails>): Promise<void> {
    if (details.connection_id !== undefined && details.connection_id !== "") {
      await this.connectionIdInput.fill(details.connection_id);
    }

    if (details.conn_type !== undefined && details.conn_type !== "") {
      const selectInput = this.connectionForm.locator('[role="combobox"]').first();

      await expect(async () => {
        await expect(selectInput).toBeEnabled();
        await selectInput.click({ force: true, timeout: 5000 });
      }).toPass({ intervals: [2000, 3000], timeout: 120_000 });

      await this.page.keyboard.type(details.conn_type);

      const option = this.page.getByRole("option", { name: new RegExp(details.conn_type, "i") }).first();

      await option.click();
    }

    if (details.host !== undefined && details.host !== "") {
      await expect(this.hostInput).toBeVisible();
      await this.hostInput.fill(details.host);
    }

    if (details.port !== undefined && details.port !== "") {
      await expect(this.portInput).toBeVisible();
      await this.portInput.fill(String(details.port));
    }

    if (details.login !== undefined && details.login !== "") {
      await expect(this.loginInput).toBeVisible();
      await this.loginInput.fill(details.login);
    }

    if (details.password !== undefined && details.password !== "") {
      await expect(this.passwordInput).toBeVisible();
      await this.passwordInput.fill(details.password);
    }

    if (details.description !== undefined && details.description !== "") {
      await expect(this.descriptionInput).toBeVisible();
      await this.descriptionInput.fill(details.description);
    }

    if (details.schema !== undefined && details.schema !== "") {
      await expect(this.schemaInput).toBeVisible();
      await this.schemaInput.fill(details.schema);
    }

    if (details.extra !== undefined && details.extra !== "") {
      const extraAccordion = this.page.getByRole("button", { name: /extra fields json/i });
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
    const rowLocator = this.page.locator("tbody tr");
    const stableRowCount = await waitForStableRowCount(rowLocator).catch(() => 0);

    if (stableRowCount === 0) {
      return [];
    }

    const headerTexts = await this.page.locator("thead th").allTextContents();
    const idColumnIndex = headerTexts.findIndex((text) => /connection\s*id/i.test(text));

    if (idColumnIndex === -1) {
      throw new Error(`"Connection ID" column not found in headers: ${JSON.stringify(headerTexts)}`);
    }

    const rows = this.page.locator("tbody tr");
    const connectionIds: Array<string> = [];

    for (let i = 0; i < stableRowCount; i++) {
      try {
        const cell = rows.nth(i).locator("td").nth(idColumnIndex);
        const text = await cell.textContent({ timeout: 3000 });

        if (text !== null && text.trim() !== "") {
          connectionIds.push(text.trim());
        }
      } catch {
        continue;
      }
    }

    return connectionIds;
  }

  public async navigate(): Promise<void> {
    await expect(async () => {
      await this.navigateTo(ConnectionsPage.connectionsListUrl);
      await this.waitForConnectionsListLoad();
    }).toPass({ intervals: [2000], timeout: 60_000 });
  }

  public async saveConnection(): Promise<void> {
    await expect(this.saveButton).toBeVisible();
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
    await expect(async () => {
      await this.searchInput.fill(searchTerm);
      await expect(this.searchInput).toHaveValue(searchTerm);
    }).toPass({ intervals: [1000, 2000], timeout: 30_000 });

    await expect
      .poll(
        async () => {
          const ids = await this.getConnectionIds();
          const isEmptyVisible = await this.emptyState.isVisible().catch(() => false);

          if (isEmptyVisible) {
            return ids.length === 0;
          }

          if (ids.length === 0) {
            return false;
          }

          if (searchTerm === "") {
            return ids.length > 0;
          }

          return ids.every((id) => id.toLowerCase().includes(searchTerm.toLowerCase()));
        },
        { message: "Search results did not match search term", timeout: 30_000 },
      )
      .toBeTruthy();
  }

  public async verifyConnectionInList(connectionId: string, expectedType: string): Promise<void> {
    const row = await this.findConnectionRow(connectionId);

    if (!row) {
      throw new Error(`Connection ${connectionId} not found in list`);
    }

    const rowText = await row.textContent();

    expect(rowText).toContain(connectionId);
    expect(rowText).toContain(expectedType);
  }

  private async findConnectionRow(connectionId: string): Promise<Locator | undefined> {
    const hasSearch = await this.searchInput.isVisible({ timeout: 500 }).catch(() => false);

    if (hasSearch) {
      return await this.findConnectionRowUsingSearch(connectionId);
    }

    return undefined;
  }

  private async findConnectionRowUsingSearch(connectionId: string): Promise<Locator | undefined> {
    await this.searchConnections(connectionId);

    const isTableVisible = await this.connectionsTable.isVisible({ timeout: 5000 }).catch(() => false);

    if (!isTableVisible) {
      return undefined;
    }

    const row = this.page.locator("tbody tr").filter({ hasText: connectionId }).first();

    const rowExists = await row.isVisible({ timeout: 3000 }).catch(() => false);

    if (!rowExists) {
      return undefined;
    }

    return row;
  }

  private async waitForConnectionsListLoad(): Promise<void> {
    await expect(this.page).toHaveURL(/\/connections/);
    await this.page.waitForLoadState("domcontentloaded");

    const table = this.connectionsTable;

    await expect(table.or(this.emptyState)).toBeVisible();

    const isTableVisible = await table.isVisible();

    if (isTableVisible) {
      const firstRow = this.page.locator("tbody tr").first();

      await expect(firstRow.or(this.emptyState)).toBeVisible({ timeout: 15_000 });
    }
  }
}
