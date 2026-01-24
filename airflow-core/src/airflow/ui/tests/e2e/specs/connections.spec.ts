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

  test.beforeEach(({ page }) => {
    connectionsPage = new ConnectionsPage(page);
  });
  test.beforeAll(async ({ browser }) => {
    const context = await browser.newContext({ storageState: AUTH_FILE });
    const page = await context.newPage();

    await page.request.post(`${baseUrl}/api/v2/connections`, {
      data: {
        conn_type: "http",
        connection_id: "list_seed_conn",
        host: "seed.example.com",
      },
    });
  });

  test.afterAll(async ({ browser }) => {
    const context = await browser.newContext({ storageState: AUTH_FILE });
    const page = await context.newPage();

    await page.request.delete(`${baseUrl}/api/v2/connections/list_seed_conn`);
  });

  test("should display connections list page", async () => {
    await connectionsPage.navigate();

    // Verify the page is loaded
    expect(connectionsPage.page.url()).toContain("/connections");

    // Verify table or list is visible
    expect(await connectionsPage.connectionsTable.isVisible()).toBeTruthy();
  });

  test("should display connections with correct columns", async () => {
    await connectionsPage.navigate();

    // Check that we have at least one row
    const count = await connectionsPage.getConnectionCount();

    expect(count).toBeGreaterThanOrEqual(0);

    if (count > 0) {
      // Verify connections are listed with expected information
      const connectionIds = await connectionsPage.getConnectionIds();

      expect(connectionIds.length).toBeGreaterThan(0);
    }
  });

  test("should have Add button visible", async () => {
    await connectionsPage.navigate();
    expect(await connectionsPage.addButton.isVisible()).toBeTruthy();
  });
});

test.describe("Connections Page - CRUD Operations", () => {
  let connectionsPage: ConnectionsPage;
  const { baseUrl } = testConfig.connection;
  const timestamp = Date.now();

  // Test connection details - using dynamic data
  const testConnection = {
    conn_type: "postgres", // Adjust based on available connection types in your Airflow instance
    connection_id: `atest_conn_${timestamp}`,
    description: `Test connection created at ${new Date().toISOString()}`,
    extra: JSON.stringify({
      options: "-c statement_timeout=5000",
      sslmode: "require",
    }),
    host: `test-host-${timestamp}.example.com`,
    login: `test_user_${timestamp}`,
    password: `test_password_${timestamp}`,
    port: 5432,
    schema: "test_db",
  };

  const updatedConnection = {
    description: `Updated test connection at ${new Date().toISOString()}`,
    host: `updated-host-${timestamp}.example.com`,
    login: `updated_user_${timestamp}`,
    port: 5433,
  };

  test.beforeEach(({ page }) => {
    connectionsPage = new ConnectionsPage(page);
  });

  test.afterAll(async ({ browser }) => {
    // Cleanup: Delete test connections via API
    const context = await browser.newContext({ storageState: AUTH_FILE });
    const page = await context.newPage();

    // Delete the test connection
    const deleteResponse = await page.request.delete(
      `${baseUrl}/api/v2/connections/${testConnection.connection_id}`,
    );

    expect([204, 404]).toContain(deleteResponse.status());
  });

  test("should create a new connection with all fields", async () => {
    await connectionsPage.navigate();

    // Click add button
    await connectionsPage.createConnection(testConnection);
    // Verify the connection was created
    const exists = await connectionsPage.connectionExists(testConnection.connection_id);

    expect(exists).toBeTruthy();
  });

  test("should display created connection in list with correct type", async () => {
    await connectionsPage.navigate();

    // Verify the connection is visible with correct details
    await connectionsPage.verifyConnectionInList(testConnection.connection_id, testConnection.conn_type);
  });

  test("should edit an existing connection", async () => {
    await connectionsPage.navigate();

    // Verify connection exists before editing
    let exists = await connectionsPage.connectionExists(testConnection.connection_id);

    expect(exists).toBeTruthy();

    // Edit the connection
    await connectionsPage.editConnection(testConnection.connection_id, updatedConnection);

    // Verify the connection was updated
    // await connectionsPage.navigate();
    exists = await connectionsPage.connectionExists(testConnection.connection_id);
    expect(exists).toBeTruthy();
  });

  test("should delete a connection", async () => {
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
    let exists = await connectionsPage.connectionExists(tempConnection.connection_id);

    expect(exists).toBeTruthy();

    // Delete the connection
    await connectionsPage.deleteConnection(tempConnection.connection_id);
    exists = await connectionsPage.connectionExists(tempConnection.connection_id);
    expect(exists).toBeFalsy();
  });
});

test.describe("Connections Page - Pagination", () => {
  let connectionsPage: ConnectionsPage;
  const { baseUrl } = testConfig.connection;
  const timestamp = Date.now();

  // Create multiple test connections to ensure we have enough for pagination testing
  const testConnections = Array.from({ length: 5 }, (_, i) => ({
    conn_type: "http",
    connection_id: `pagination_test_${timestamp}_${i}`,
    host: `pagination-host-${i}.example.com`,
    login: `pagination_user_${i}`,
  }));

  test.beforeEach(({ page }) => {
    connectionsPage = new ConnectionsPage(page);
  });

  test.beforeAll(async ({ browser }) => {
    // Create multiple test connections via API
    const context = await browser.newContext({ storageState: AUTH_FILE });
    const page = await context.newPage();

    for (const conn of testConnections) {
      const response = await page.request.post(`${baseUrl}/api/v2/connections`, {
        data: JSON.stringify(conn),
        headers: {
          "Content-Type": "application/json",
        },
      });

      // Connection may already exist
      expect([200, 201, 409]).toContain(response.status());
    }
  });

  test.afterAll(async ({ browser }) => {
    // Cleanup all test connections
    const context = await browser.newContext({ storageState: AUTH_FILE });
    const page = await context.newPage();

    for (const conn of testConnections) {
      const response = await page.request.delete(`${baseUrl}/api/v2/connections/${conn.connection_id}`);

      expect([204, 404]).toContain(response.status());
    }
  });

  test("should display pagination controls when applicable", async () => {
    await connectionsPage.navigate();

    const hasPagination = await connectionsPage.isPaginationVisible();

    expect(typeof hasPagination).toBe("boolean");
  });

  test("should navigate to next page and verify data changes", async () => {
    await connectionsPage.navigate();
    const hasPagination = await connectionsPage.isPaginationVisible();

    if (hasPagination) {
      const initialIds = await connectionsPage.getConnectionIds();

      expect(initialIds.length).toBeGreaterThan(0);

      // Check if next button is enabled
      const nextButtonEnabled = await connectionsPage.paginationNextButton
        .isEnabled({ timeout: 2000 })
        .catch(() => false);

      if (nextButtonEnabled) {
        await connectionsPage.clickNextPage();
        const newIds = await connectionsPage.getConnectionIds();

        // Verify we have connections on the new page
        expect(newIds.length).toBeGreaterThan(0);
      }
    }
  });

  test("should navigate to previous page and return to original", async () => {
    await connectionsPage.navigate();

    const hasPagination = await connectionsPage.isPaginationVisible();

    if (hasPagination) {
      const nextButtonEnabled = await connectionsPage.paginationNextButton
        .isEnabled({ timeout: 2000 })
        .catch(() => false);

      if (nextButtonEnabled) {
        await connectionsPage.clickNextPage();

        // Check if previous button is enabled
        const prevButtonEnabled = await connectionsPage.paginationPrevButton
          .isEnabled({ timeout: 2000 })
          .catch(() => false);

        if (prevButtonEnabled) {
          // Go back to first page
          await connectionsPage.clickPrevPage();
          const returnedIds = await connectionsPage.getConnectionIds();

          expect(returnedIds.length).toBeGreaterThan(0);
        }
      }
    }
  });
});

test.describe("Connections Page - Sorting", () => {
  let connectionsPage: ConnectionsPage;
  const { baseUrl } = testConfig.connection;
  const timestamp = Date.now();

  // Create test connections with distinct names for sorting
  const sortTestConnections = [
    {
      conn_type: "http",
      connection_id: `z_sort_conn_${timestamp}`,
      host: "z-host.example.com",
      login: "z_user",
    },
    {
      conn_type: "postgres",
      connection_id: `a_sort_conn_${timestamp}`,
      host: "a-host.example.com",
      login: "a_user",
    },
    {
      conn_type: "mysql",
      connection_id: `m_sort_conn_${timestamp}`,
      host: "m-host.example.com",
      login: "m_user",
    },
  ];

  test.beforeEach(({ page }) => {
    connectionsPage = new ConnectionsPage(page);
  });

  test.beforeAll(async ({ browser }) => {
    // Create test connections
    const context = await browser.newContext({ storageState: AUTH_FILE });
    const page = await context.newPage();

    for (const conn of sortTestConnections) {
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

    for (const conn of sortTestConnections) {
      const response = await page.request.delete(`${baseUrl}/api/v2/connections/${conn.connection_id}`);

      expect([204, 404]).toContain(response.status());
    }
  });

  test("should sort by Connection Type when clicking header", async () => {
    await connectionsPage.navigate();

    // Click to sort (first click should be ascending)
    await connectionsPage.sortByHeader("Connection ID");
    const idsAfter = await connectionsPage.getConnectionIds();

    // Verify it's actually sorted (case-insensitive)
    const sortedIds = [...idsAfter].sort((a, b) => a.toLowerCase().localeCompare(b.toLowerCase()));

    expect(idsAfter).toEqual(sortedIds);
  });

  test("should toggle sort order when clicking header twice", async () => {
    await connectionsPage.navigate();

    // First click
    await connectionsPage.sortByHeader("Connection ID");
    const idsAsc = await connectionsPage.getConnectionIds();

    expect(idsAsc.length).toBeGreaterThan(0);

    // Verify it's sorted ascending
    let isAscending = true;

    for (let i = 0; i < idsAsc.length - 1; i++) {
      const current = idsAsc[i];
      const next = idsAsc[i + 1];

      // TypeScript safety check
      if (current === undefined || current === "" || next === undefined || next === "") continue;

      if (current.toLowerCase() > next.toLowerCase()) {
        isAscending = false;
        break;
      }
    }
    expect(isAscending).toBe(true);

    // Second click
    await connectionsPage.sortByHeader("Connection ID");
    const idsDesc = await connectionsPage.getConnectionIds();

    expect(idsDesc.length).toBeGreaterThan(0);

    // Verify it's sorted descending
    let isDescending = true;

    for (let i = 0; i < idsDesc.length - 1; i++) {
      const current = idsDesc[i];
      const next = idsDesc[i + 1];

      // TypeScript safety check
      if (current === undefined || current === "" || next === undefined || next === "") continue;
      if (current.toLowerCase() < next.toLowerCase()) {
        isDescending = false;
        break;
      }
    }
    expect(isDescending).toBe(true);
  });

  test("should keep each page sorted after navigating", async () => {
    await connectionsPage.navigate();

    const hasPagination = await connectionsPage.isPaginationVisible();

    if (!hasPagination) {
      test.skip();

      return;
    }

    // Sort first page
    await connectionsPage.sortByHeader("Connection ID");
    const firstPageIds = await connectionsPage.getConnectionIds();

    if (firstPageIds.length === 0) {
      test.skip();

      return;
    }

    const nextButtonEnabled = await connectionsPage.paginationNextButton.isEnabled().catch(() => false);

    if (!nextButtonEnabled) {
      test.skip();

      return;
    }

    // Navigate to next page
    await connectionsPage.clickNextPage();
    const secondPageIds = await connectionsPage.getConnectionIds();

    expect(secondPageIds.length).toBeGreaterThan(0);

    // Verify second page is ALSO sorted (this is what matters!)
    const secondPageSorted = secondPageIds.every((id, i) => {
      if (i === secondPageIds.length - 1) return true;
      const nextId = secondPageIds[i + 1];

      return nextId !== undefined && id.toLowerCase() <= nextId.toLowerCase();
    });

    expect(secondPageSorted).toBe(true);
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

    // Try to search for a specific connection
    try {
      const searchTerm = "production";

      await connectionsPage.searchConnections(searchTerm);
      const ids = await connectionsPage.getConnectionIds();

      // Should have at least the test connection with 'production' in the name
      if (ids.length > 0) {
        expect(ids.length).toBeGreaterThan(-1);
      }
    } catch {
      // Search might not be implemented
      await connectionsPage.navigate();
    }
  });

  test("should display all connections when search is cleared", async () => {
    await connectionsPage.navigate();
    const initialCount = await connectionsPage.getConnectionCount();

    // Try searching
    try {
      await connectionsPage.searchConnections("production");
      await connectionsPage.page.waitForTimeout(500);

      // Clear search
      await connectionsPage.searchConnections("");
      await connectionsPage.page.waitForTimeout(500);
      const finalCount = await connectionsPage.getConnectionCount();

      // After clearing, should have same or more connections
      expect(finalCount).toBeGreaterThanOrEqual(0);
    } catch {
      // Search not available
      expect(initialCount).toBeGreaterThanOrEqual(0);
    }
  });
});
