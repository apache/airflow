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
import { testConfig } from "playwright.config";
import { DagsPage } from "tests/e2e/pages/DagsPage";
import { LoginPage } from "tests/e2e/pages/LoginPage";

/**
 * DAGS-001: Verify DAGs List Displays
 *
 * This test verifies that the DAGs list page displays correctly after login,
 * including the presence of the DAGs table and at least one DAG entry.
 */

test.describe("DAGs List Display", () => {
  let loginPage: LoginPage;
  let dagsPage: DagsPage;

  // Test configuration from centralized config
  const testCredentials = testConfig.credentials;

  test.beforeEach(({ page }) => {
    loginPage = new LoginPage(page);
    dagsPage = new DagsPage(page);
  });

  test("should display DAGs list after successful login", async () => {
    test.setTimeout(2 * 60 * 1000); // 2 minutes timeout
    // Step 1: Login to Airflow
    await loginPage.navigateAndLogin(testCredentials.username, testCredentials.password);

    // Step 2: Verify login was successful
    await loginPage.expectLoginSuccess();

    // Step 3: Navigate to DAGs list page
    await dagsPage.navigate();

    // Step 4: Verify DAGs list/table is visible
    await dagsPage.verifyDagsListVisible();

    // Step 5: Verify at least one DAG is displayed
    const dagsCount = await dagsPage.getDagsCount();

    expect(dagsCount).toBeGreaterThan(0);
  });

  test("should display DAG links correctly", async () => {
    test.setTimeout(2 * 60 * 1000); // 2 minutes timeout
    // Step 1: Login to Airflow
    await loginPage.navigateAndLogin(testCredentials.username, testCredentials.password);

    await loginPage.expectLoginSuccess();

    // Step 2: Navigate to DAGs list page
    await dagsPage.navigate();

    // Step 3: Verify DAGs list is visible
    await dagsPage.verifyDagsListVisible();

    // Step 4: Get all DAG links
    const dagLinks = await dagsPage.getDagLinks();

    // Step 5: Verify links exist and have correct format
    expect(dagLinks.length).toBeGreaterThan(0);

    // Verify each link has the correct format (/dags/{dag_id})
    for (const link of dagLinks) {
      expect(link).toMatch(/\/dags\/.+/);
    }
  });

  test("should display test DAG in the list", async () => {
    test.setTimeout(2 * 60 * 1000); // 2 minutes timeout
    const testDagId = testConfig.testDag.id;

    // Step 1: Login to Airflow
    await loginPage.navigateAndLogin(testCredentials.username, testCredentials.password);

    await loginPage.expectLoginSuccess();

    // Step 2: Navigate to DAGs list page
    await dagsPage.navigate();

    // Step 3: Verify DAGs list is visible
    await dagsPage.verifyDagsListVisible();

    // Step 4: Verify the test DAG exists in the list
    const dagExists = await dagsPage.verifyDagExists(testDagId);

    expect(dagExists).toBe(true);
  });
});
