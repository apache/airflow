/* eslint-disable unicorn/no-null */

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
import { test, expect } from '@playwright/test';
import { LoginPage } from '../pages/LoginPage';
import { DagsPage } from '../pages/DagsPage';
import { testConfig } from '../../../playwright.config';

/**
 * DAG Trigger E2E Tests
 */

test.describe('DAG Trigger Workflow', () => {
  let loginPage: LoginPage;
  let dagsPage: DagsPage;

  // Test configuration from centralized config
  const testCredentials = testConfig.credentials;
  const testDagId = testConfig.testDag.id;

  test.beforeEach(async ({ page }) => {
    loginPage = new LoginPage(page);
    dagsPage = new DagsPage(page);
  });

  test('should successfully trigger a DAG run', async ({ page }) => {
    test.setTimeout(7 * 60 * 1000);

    await loginPage.navigateAndLogin(testCredentials.username, testCredentials.password);
    await loginPage.expectLoginSuccess();

    const dagRunId = await dagsPage.triggerDag(testDagId);
    if (dagRunId) {
      await dagsPage.verifyDagRunStatus(testDagId, dagRunId);
    }
  })
});
