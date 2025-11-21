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
import { test } from "@playwright/test";

import { testConfig } from "../../../playwright.config";
import { DagsPage } from "../pages/DagsPage";
import { LoginPage } from "../pages/LoginPage";

/**
 * DAG Trigger E2E Tests
 */

test.describe("DAG Trigger Workflow", () => {
  let loginPage: LoginPage;
  let dagsPage: DagsPage;

  // Test configuration from centralized config
  // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment, @typescript-eslint/no-unsafe-member-access
  const testCredentials = testConfig.credentials;
  // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment, @typescript-eslint/no-unsafe-member-access
  const testDagId = testConfig.testDag.id;

  // eslint-disable-next-line @typescript-eslint/require-await
  test.beforeEach(async ({ page }) => {
    // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment, @typescript-eslint/no-unsafe-call
    loginPage = new LoginPage(page);
    // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment, @typescript-eslint/no-unsafe-call
    dagsPage = new DagsPage(page);
  });

  test("should successfully trigger a DAG run", async () => {
    test.setTimeout(7 * 60 * 1000);

    // eslint-disable-next-line @typescript-eslint/no-unsafe-call, @typescript-eslint/no-unsafe-member-access
    await loginPage.navigateAndLogin(testCredentials.username, testCredentials.password);
    // eslint-disable-next-line @typescript-eslint/no-unsafe-call, @typescript-eslint/no-unsafe-member-access
    await loginPage.expectLoginSuccess();

    // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment, @typescript-eslint/no-unsafe-call, @typescript-eslint/no-unsafe-member-access
    const dagRunId = await dagsPage.triggerDag(testDagId);

    // eslint-disable-next-line @typescript-eslint/strict-boolean-expressions
    if (dagRunId) {
      // eslint-disable-next-line @typescript-eslint/no-unsafe-call, @typescript-eslint/no-unsafe-member-access
      await dagsPage.verifyDagRunStatus(testDagId, dagRunId);
    }
  });
});
