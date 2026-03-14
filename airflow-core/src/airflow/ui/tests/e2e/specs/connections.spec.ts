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
import { expect, test } from "@playwright/test";
import { testConfig, AUTH_FILE } from "playwright.config";
import { ConnectionsPage } from "tests/e2e/pages/ConnectionsPage";

test.describe("Connections Page - List and Display", () => {
  let connectionsPage: ConnectionsPage;
  const { baseUrl } = testConfig.connection;
  const timestamp = Date.now();
  const seedConnection = {
    conn_type: "http",
    connection_id: `list_seed_conn_${timestamp}`,
    host: "seed.example.com",
  };

  test.beforeEach(({ page }) => {
    connectionsPage = new ConnectionsPage(page);
  });

  test.beforeAll(async ({ browser }) => {
    const context = await browser.newContext({ storageState: AUTH_FILE });
    const page = await context.newPage();

    const response = await page.request.post(`${baseUrl}/api/v2/connections`, {
      data: seedConnection,
      headers: { "Content-Type": "application/json" },
    });

    expect([200, 201, 409]).toContain(response.status());
    await context.close();
  });

  test.afterAll(async ({ browser }) => {
    const context = await browser.newContext({ storageState: AUTH_FILE });
    const page = await context.newPage();

    const response = await page.request.delete(`${baseUrl}/api/v2/connections/list_seed_conn_${timestamp}`);

    expect([204, 404]).toContain(response.status());
    await context.close();
  });

  test("should display connections list page", async () => {
    await connectionsPage.navigate();

    // Verify the page is loaded
    await expect(connectionsPage.page).toHaveURL(/\/connections/);

    // Verify table or list is visible
    await expect(connectionsPage.connectionsTable).toBeVisible();
  });

  test("should display connections with correct columns", async () => {
    await connectionsPage.navigate();

    // Check that we have at least one row
    await expect(connectionsPage.connectionRows).not.toHaveCount(0);

    // Verify column headers exist
    await expect(connectionsPage.connectionIdHeader).toBeVisible();
    await expect(connectionsPage.connectionTypeHeader).toBeVisible();
    await expect(connectionsPage.hostHeader).toBeVisible();

    // Verify the seed connection is displayed in the list
    await connectionsPage.verifyConnectionInList(seedConnection.connection_id, seedConnection.conn_type);
  });

  test("should have Add button visible", async () => {
    await connectionsPage.navigate();
    await expect(connectionsPage.addButton).toBeVisible();
  });
});

test.describe("Connections Page - CRUD Operations", () => {
  let connectionsPage: ConnectionsPage;
  const { baseUrl } = testConfig.connection;
  const timestamp = Date.now();

  // Connection created via API in beforeAll - used for edit and display tests
  const existingConnection = {
    conn_type: "postgres",
    connection_id: `existing_conn_${timestamp}`,
    host: `existing-host-${timestamp}.example.com`,
    login: `existing_user_${timestamp}`,
  };

  const updatedConnection = {
    conn_type: "postgres",
    description: `Updated test connection at ${new Date().toISOString()}`,
    host: `updated-host-${timestamp}.example.com`,
    login: `updated_user_${timestamp}`,
    port: 5433,
  };

  // Connection created via UI in test - used for create and delete tests
  const newConnection = {
    conn_type: "postgres",
    connection_id: `new_conn_${timestamp}`,
    description: `Test connection created at ${new Date().toISOString()}`,
    extra: JSON.stringify({
      options: "-c statement_timeout=5000",
      sslmode: "require",
    }),
    host: `new-host-${timestamp}.example.com`,
    login: `new_user_${timestamp}`,
    password: `new_password_${timestamp}`,
    port: 5432,
    schema: "test_db",
  };

  test.beforeEach(({ page }) => {
    connectionsPage = new ConnectionsPage(page);
  });

  test.beforeAll(async ({ browser }) => {
    // Create existing connection via API for edit and display tests
    const context = await browser.newContext({ storageState: AUTH_FILE });
    const page = await context.newPage();

    await page.request.post(`${baseUrl}/api/v2/connections`, {
      data: existingConnection,
      headers: { "Content-Type": "application/json" },
    });

    await context.close();
  });

  test.afterAll(async ({ browser }) => {
    // Cleanup all test connections via API
    const context = await browser.newContext({ storageState: AUTH_FILE });
    const page = await context.newPage();

    for (const connId of [
      existingConnection.connection_id,
      newConnection.connection_id,
      `temp_conn_${timestamp}_delete`,
    ]) {
      await page.request.delete(`${baseUrl}/api/v2/connections/${connId}`);
    }

    await context.close();
  });

  test("should create a new connection and display it in list", async () => {
    test.setTimeout(120_000);
    await connectionsPage.navigate();

    // Create connection via UI
    await connectionsPage.createConnection(newConnection);

    // Verify it appears in the list with correct type (web-first assertion)
    await connectionsPage.verifyConnectionInList(newConnection.connection_id, newConnection.conn_type);
  });

  test("should edit an existing connection", async () => {
    test.setTimeout(120_000);
    await connectionsPage.navigate();

    // Verify connection exists before editing (web-first assertion)
    await expect(connectionsPage.getConnectionRow(existingConnection.connection_id)).toBeVisible();

    // Edit the connection
    await connectionsPage.editConnection(existingConnection.connection_id, updatedConnection);

    // Verify the connection still exists after editing (web-first assertion)
    await expect(connectionsPage.getConnectionRow(existingConnection.connection_id)).toBeVisible();
  });

  test("should delete a connection", async () => {
    test.setTimeout(120_000);

    // Create a temporary connection for deletion test
    const tempConnection = {
      conn_type: "postgres",
      connection_id: `temp_conn_${timestamp}_delete`,
      host: `temp-host-${timestamp}.example.com`,
      login: "temp_user",
      password: "temp_password",
    };

    await connectionsPage.navigate();
    await connectionsPage.createConnection(tempConnection);

    // Verify it exists before deleting (web-first assertion)
    await expect(connectionsPage.getConnectionRow(tempConnection.connection_id)).toBeVisible();

    // Delete the connection
    await connectionsPage.deleteConnection(tempConnection.connection_id);

    // Verify it is gone (web-first assertion)
    await expect(connectionsPage.getConnectionRow(tempConnection.connection_id)).not.toBeVisible();
  });
});

test.describe("Connections Page - Search and Filter", () => {
  let connectionsPage: ConnectionsPage;
  const { baseUrl } = testConfig.connection;
  const timestamp = Date.now();

  const searchTestConnections = [
    {
      conn_type: "postgres",
      connection_id: `search_production_${timestamp}`,
      host: "prod-db.example.com",
      login: "prod_user",
    },
    {
      conn_type: "mysql",
      connection_id: `search_staging_${timestamp}`,
      host: "staging-db.example.com",
      login: "staging_user",
    },
    {
      conn_type: "http",
      connection_id: `search_development_${timestamp}`,
      host: "dev-api.example.com",
      login: "dev_user",
    },
  ];

  test.beforeEach(({ page }) => {
    connectionsPage = new ConnectionsPage(page);
  });

  test.beforeAll(async ({ browser }) => {
    // Create test connections
    const context = await browser.newContext({ storageState: AUTH_FILE });
    const page = await context.newPage();

    for (const conn of searchTestConnections) {
      const response = await page.request.post(`${baseUrl}/api/v2/connections`, {
        data: JSON.stringify(conn),
        headers: {
          "Content-Type": "application/json",
        },
      });

      expect([200, 201, 409]).toContain(response.status());
    }
  });

  test.afterAll(async ({ browser }) => {
    // Cleanup
    const context = await browser.newContext({ storageState: AUTH_FILE });
    const page = await context.newPage();

    for (const conn of searchTestConnections) {
      const response = await page.request.delete(`${baseUrl}/api/v2/connections/${conn.connection_id}`);

      expect([204, 404]).toContain(response.status());
    }
  });

  test("should filter connections by search term", async () => {
    await connectionsPage.navigate();

    const targetConnection = searchTestConnections[0]!;
    const searchTerm = targetConnection.connection_id;

    // Check that we have at least one row before searching (web-first assertion)
    await connectionsPage.searchConnections(searchTerm);

    // Verify filtered results contain the search term
    await expect(connectionsPage.connectionRows).toHaveCount(1);
    await expect(connectionsPage.getConnectionRow(searchTerm)).toBeVisible();
  });

  test("should display all connections when search is cleared", async () => {
    test.setTimeout(120_000);
    await connectionsPage.navigate();

    // Verify rows exist before searching (web-first assertion)
    await expect(connectionsPage.connectionRows).not.toHaveCount(0);
    const initialCount = await connectionsPage.getConnectionCount();

    // Search for something and wait for results
    await connectionsPage.searchConnections("production");
    await expect(connectionsPage.connectionRows).not.toHaveCount(0);

    // Clear search and verify at least as many rows as before
    await connectionsPage.searchConnections("");
    await expect(connectionsPage.connectionRows).not.toHaveCount(0);

    const finalCount = await connectionsPage.getConnectionCount();

    expect(finalCount).toBeGreaterThanOrEqual(initialCount);
  });
});
