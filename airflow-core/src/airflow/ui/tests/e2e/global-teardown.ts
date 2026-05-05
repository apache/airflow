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
import { request } from "@playwright/test";

import { AUTH_FILE, testConfig } from "../../playwright.config";

/**
 * Re-pause all Dags that E2E fixtures may have unpaused during the test run.
 * Runs once after all workers have finished, preventing the scheduler from
 * creating unbounded Dag runs in shared environments.
 */
async function globalTeardown() {
  const baseURL = testConfig.connection.baseUrl;

  const context = await request.newContext({
    baseURL,
    storageState: AUTH_FILE,
  });

  const dagIds = [
    testConfig.testDag.id,
    testConfig.testDag.hitlId,
    testConfig.xcomDag.id,
    "asset_produces_1",
  ];

  for (const dagId of dagIds) {
    try {
      await context.patch(`/api/v2/dags/${dagId}`, {
        data: { is_paused: true },
        headers: { "Content-Type": "application/json" },
        timeout: 10_000,
      });
    } catch (error) {
      console.debug(
        `[e2e teardown] Could not re-pause Dag ${dagId}: ${error instanceof Error ? error.message : String(error)}`,
      );
    }
  }

  await context.dispose();
}

export default globalTeardown;
