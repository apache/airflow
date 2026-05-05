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
import { expect, test } from "tests/e2e/fixtures";
import { uniqueRunId } from "tests/e2e/utils/test-helpers";

test.describe("Connections Page - List and Display", () => {
  let seedConnection: { conn_type: string; connection_id: string; host: string };

  test.beforeAll(async ({ authenticatedRequest }) => {
    seedConnection = {
      conn_type: "http",
      connection_id: `list_seed_conn_${uniqueRunId("t")}`,
      host: "seed.example.com",
    };

    const response = await authenticatedRequest.post(`/api/v2/connections`, {
      data: seedConnection,
      headers: { "Content-Type": "application/json" },
      timeout: 30_000,
    });

    expect([200, 201, 409]).toContain(response.status());
  });

  test.afterAll(async ({ authenticatedRequest }) => {
    const response = await authenticatedRequest.delete(
      `/api/v2/connections/${seedConnection.connection_id}`,
      { timeout: 30_000 },
    );

    if (response.status() !== 404 && !response.ok()) {
      console.warn(`Cleanup failed for ${seedConnection.connection_id}: ${response.status()}`);
    }
  });

  test("should display connections list page", async ({ connectionsPage }) => {
    await connectionsPage.navigate();

    expect(connectionsPage.page.url()).toContain("/connections");

    await expect(connectionsPage.connectionsTable).toBeVisible();
  });

  test("should display connections with correct columns", async ({ connectionsPage }) => {
    await connectionsPage.navigate();

    await expect
      .poll(async () => connectionsPage.getConnectionCount(), { intervals: [1000], timeout: 15_000 })
      .toBeGreaterThan(0);

    await expect(connectionsPage.connectionIdHeader).toBeVisible();
    await expect(connectionsPage.connectionTypeHeader).toBeVisible();
    await expect(connectionsPage.hostHeader).toBeVisible();

    await connectionsPage.verifyConnectionInList(seedConnection.connection_id, seedConnection.conn_type);
  });

  test("should have Add button visible", async ({ connectionsPage }) => {
    await connectionsPage.navigate();
    await expect(connectionsPage.addButton).toBeVisible();
  });
});

test.describe("Connections Page - CRUD Operations", () => {
  let existingConnection: { conn_type: string; connection_id: string; host: string; login: string };
  let updatedConnection: {
    conn_type: string;
    description: string;
    host: string;
    login: string;
    port: number;
  };

  const createdConnIds: Array<string> = [];

  test.beforeAll(async ({ authenticatedRequest }) => {
    const timestamp = uniqueRunId("t");

    existingConnection = {
      conn_type: "postgres",
      connection_id: `existing_conn_${timestamp}`,
      host: `existing-host-${timestamp}.example.com`,
      login: `existing_user_${timestamp}`,
    };

    updatedConnection = {
      conn_type: "postgres",
      description: `Updated test connection at ${new Date().toISOString()}`,
      host: `updated-host-${timestamp}.example.com`,
      login: `updated_user_${timestamp}`,
      port: 5433,
    };

    await authenticatedRequest.post(`/api/v2/connections`, {
      data: existingConnection,
      headers: { "Content-Type": "application/json" },
      timeout: 30_000,
    });
  });

  test.afterAll(async ({ authenticatedRequest }) => {
    for (const connId of [existingConnection.connection_id, ...createdConnIds]) {
      const response = await authenticatedRequest.delete(`/api/v2/connections/${connId}`, {
        timeout: 30_000,
      });

      if (response.status() !== 404 && !response.ok()) {
        console.warn(`Cleanup failed for ${connId}: ${response.status()}`);
      }
    }
  });

  test("should create a new connection and display it in list", async ({ connectionsPage }) => {
    test.slow();

    const connId = `new_conn_${uniqueRunId("create")}`;

    createdConnIds.push(connId);

    const newConnection = {
      conn_type: "postgres",
      connection_id: connId,
      description: `Test connection created at ${new Date().toISOString()}`,
      extra: JSON.stringify({
        options: "-c statement_timeout=5000",
        sslmode: "require",
      }),
      host: `new-host-${connId}.example.com`,
      login: `new_user_${connId}`,
      password: `new_password_${connId}`,
      port: 5432,
      schema: "test_db",
    };

    await connectionsPage.navigate();
    await connectionsPage.createConnection(newConnection);

    const exists = await connectionsPage.connectionExists(newConnection.connection_id);

    expect(exists).toBeTruthy();
    await connectionsPage.verifyConnectionInList(newConnection.connection_id, newConnection.conn_type);
  });

  test("should edit an existing connection", async ({ connectionsPage }) => {
    test.slow();
    await connectionsPage.navigate();

    const exists = await connectionsPage.connectionExists(existingConnection.connection_id);

    expect(exists).toBeTruthy();

    await connectionsPage.editConnection(existingConnection.connection_id, updatedConnection);

    const stillExists = await connectionsPage.connectionExists(existingConnection.connection_id);

    expect(stillExists).toBeTruthy();
  });

  test("should delete a connection", async ({ connectionsPage }) => {
    test.slow();

    const tempConnId = `temp_conn_${uniqueRunId("del")}`;

    const tempConnection = {
      conn_type: "postgres",
      connection_id: tempConnId,
      host: "temp-host.example.com",
      login: "temp_user",
      password: "temp_password",
    };

    await connectionsPage.navigate();
    await connectionsPage.createConnection(tempConnection);

    const exists = await connectionsPage.connectionExists(tempConnection.connection_id);

    expect(exists).toBeTruthy();

    await connectionsPage.deleteConnection(tempConnection.connection_id);

    const stillExists = await connectionsPage.connectionExists(tempConnection.connection_id);

    expect(stillExists).toBeFalsy();
  });
});

test.describe("Connections Page - Search and Filter", () => {
  let searchTestConnections: Array<{
    conn_type: string;
    connection_id: string;
    host: string;
    login: string;
  }>;

  test.beforeAll(async ({ authenticatedRequest }) => {
    const timestamp = uniqueRunId("t");

    searchTestConnections = [
      {
        conn_type: "postgres",
        connection_id: `production_search_${timestamp}`,
        host: "prod-db.example.com",
        login: "prod_user",
      },
      {
        conn_type: "mysql",
        connection_id: `staging_search_${timestamp}`,
        host: "staging-db.example.com",
        login: "staging_user",
      },
      {
        conn_type: "http",
        connection_id: `development_search_${timestamp}`,
        host: "dev-api.example.com",
        login: "dev_user",
      },
    ];

    for (const conn of searchTestConnections) {
      const response = await authenticatedRequest.post(`/api/v2/connections`, {
        data: JSON.stringify(conn),
        headers: {
          "Content-Type": "application/json",
        },
        timeout: 30_000,
      });

      expect([200, 201, 409]).toContain(response.status());
    }
  });

  test.afterAll(async ({ authenticatedRequest }) => {
    for (const conn of searchTestConnections) {
      const response = await authenticatedRequest.delete(`/api/v2/connections/${conn.connection_id}`, {
        timeout: 30_000,
      });

      if (response.status() !== 404 && !response.ok()) {
        console.warn(`Cleanup failed for ${conn.connection_id}: ${response.status()}`);
      }
    }
  });

  test("should filter connections by search term", async ({ connectionsPage }) => {
    await connectionsPage.navigate();

    await expect
      .poll(async () => connectionsPage.getConnectionCount(), { intervals: [1000], timeout: 30_000 })
      .toBeGreaterThan(0);

    const searchTerm = "production";

    await connectionsPage.searchConnections(searchTerm);

    await expect
      .poll(
        async () => {
          const ids = await connectionsPage.getConnectionIds();

          return ids.length > 0 && ids.every((id) => id.toLowerCase().includes(searchTerm.toLowerCase()));
        },
        { intervals: [500] },
      )
      .toBe(true);

    const filteredIds = await connectionsPage.getConnectionIds();

    expect(filteredIds.length).toBeGreaterThan(0);
    for (const id of filteredIds) {
      expect(id.toLowerCase()).toContain(searchTerm.toLowerCase());
    }
  });

  test("should display all connections when search is cleared", async ({ connectionsPage }) => {
    test.slow();
    await connectionsPage.navigate();

    let initialCount = 0;

    await expect
      .poll(
        async () => {
          initialCount = await connectionsPage.getConnectionCount();

          return initialCount;
        },
        { intervals: [1000], timeout: 30_000 },
      )
      .toBeGreaterThan(0);

    await connectionsPage.searchConnections("production");
    await expect
      .poll(
        async () => {
          const count = await connectionsPage.getConnectionCount();

          return count > 0; // Just verify we have some results
        },
        { intervals: [500] },
      )
      .toBe(true);

    await connectionsPage.searchConnections("");

    const finalCount = await connectionsPage.getConnectionCount();

    expect(finalCount).toBeGreaterThanOrEqual(initialCount);
  });
});
