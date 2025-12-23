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
import { test, expect } from "@playwright/test";
import { DagsPage } from "tests/e2e/pages/DagsPage";
import { LoginPage } from "tests/e2e/pages/LoginPage";
import { testConfig } from "playwright.config";

test.describe("DAG Code Tab", () => {
  let loginPage: LoginPage;
  let dagsPage: DagsPage;
  const testCredentials = testConfig.credentials;
  const testDagId = testConfig.testDag.id;

  test.beforeEach(({ page }) => {
    loginPage = new LoginPage(page);
    dagsPage = new DagsPage(page);
  });

  test("should display DAG code", async () => {
    await loginPage.navigateAndLogin(testCredentials.username, testCredentials.password);
    await loginPage.expectLoginSuccess();

    await dagsPage.navigateToDagDetail(testDagId);
    await dagsPage.openCodeTab();

    const codeContainer = dagsPage.getCodeContainer();
    await expect(codeContainer).toBeVisible();

    const codeText = await dagsPage.getDagCodeText();
    console.log("DAG code snippet:", codeText?.slice(0, 100)); // optional debug
  });
});
