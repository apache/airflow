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

test.describe("Pools Page", () => {
  test.setTimeout(60_000);

  let testPoolName: string;
  const testPoolSlots = 10;
  const testPoolDescription = "E2E test pool";
  const updatedSlots = 25;
  const createdPoolNames: Array<string> = [];

  test.beforeAll(async ({ authenticatedRequest }) => {
    testPoolName = `test_pool_${uniqueRunId("pool")}`;

    const response = await authenticatedRequest.post("/api/v2/pools", {
      data: {
        description: testPoolDescription,
        include_deferred: false,
        name: testPoolName,
        slots: testPoolSlots,
      },
      headers: { "Content-Type": "application/json" },
    });

    expect([200, 201, 409]).toContain(response.status());
  });

  test.afterAll(async ({ authenticatedRequest }) => {
    for (const name of [testPoolName, ...createdPoolNames]) {
      const response = await authenticatedRequest.delete(`/api/v2/pools/${encodeURIComponent(name)}`);

      if (response.status() !== 404 && !response.ok()) {
        console.warn(`Pool cleanup failed for ${name}: ${response.status()}`);
      }
    }
  });

  test("verify pools list displays with default_pool", async ({ poolsPage }) => {
    await poolsPage.navigate();
    await poolsPage.verifyPoolsListDisplays();
  });

  test("verify test pool exists in list", async ({ poolsPage }) => {
    await poolsPage.navigate();
    await poolsPage.verifyPoolExists(testPoolName);
  });

  test("verify pool slots display correctly", async ({ poolsPage }) => {
    await poolsPage.navigate();
    await poolsPage.verifyPoolSlots(testPoolName, testPoolSlots);
  });

  test("edit pool slots", async ({ poolsPage }) => {
    await poolsPage.navigate();
    await poolsPage.editPoolSlots(testPoolName, updatedSlots);
    await poolsPage.navigate();
    await poolsPage.verifyPoolSlots(testPoolName, updatedSlots);
  });

  test("verify pool usage displays", async ({ poolsPage }) => {
    await poolsPage.navigate();
    await poolsPage.verifyPoolUsageDisplays("default_pool");
  });

  test("create a new pool", async ({ poolsPage }) => {
    const poolName = `test_crud_pool_${uniqueRunId("crud")}`;

    createdPoolNames.push(poolName);

    await poolsPage.navigate();
    await poolsPage.createPool(poolName, 5, "Created pool");
    await poolsPage.navigate();
    await poolsPage.verifyPoolExists(poolName);
  });

  test("delete pool", async ({ poolsPage }) => {
    const poolName = `test_del_pool_${uniqueRunId("del")}`;

    // Create via API so we have a pool to delete via UI
    await poolsPage.page.request.post("/api/v2/pools", {
      data: { description: "To be deleted", include_deferred: false, name: poolName, slots: 5 },
      headers: { "Content-Type": "application/json" },
    });

    await poolsPage.navigate();
    await poolsPage.verifyPoolExists(poolName);
    await poolsPage.deletePool(poolName);
    await poolsPage.verifyPoolNotExists(poolName);
  });
});
