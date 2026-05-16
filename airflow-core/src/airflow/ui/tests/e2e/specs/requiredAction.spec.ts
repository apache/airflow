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
import { testConfig } from "playwright.config";
import { expect, test } from "tests/e2e/fixtures";
import { apiDeleteDagRun, setupHITLFlowViaAPI } from "tests/e2e/utils/test-helpers";

const hitlDagId = testConfig.testDag.hitlId;

const beforeAllRunIds: Array<string> = [];

test.describe("Verify Required Action page", () => {
  test.describe.configure({ mode: "serial" });
  test.slow();

  test.beforeAll(async ({ authenticatedRequest }) => {
    test.setTimeout(600_000);

    beforeAllRunIds.push(await setupHITLFlowViaAPI(authenticatedRequest, hitlDagId, true));
    beforeAllRunIds.push(await setupHITLFlowViaAPI(authenticatedRequest, hitlDagId, false));
  });

  test.afterAll(async ({ authenticatedRequest }) => {
    for (const runId of beforeAllRunIds) {
      await apiDeleteDagRun(authenticatedRequest, hitlDagId, runId).catch(() => undefined);
    }
  });

  test("Verify the actions table displays with expected columns", async ({ page, requiredActionsPage }) => {
    await requiredActionsPage.navigateToRequiredActionsPage();

    await expect(requiredActionsPage.actionsTable).toBeVisible();

    await expect(page.locator("th").filter({ hasText: "Dag ID" })).toBeVisible();
    await expect(page.locator("th").filter({ hasText: "Task ID" })).toBeVisible();
    await expect(page.locator("th").filter({ hasText: "Dag Run ID" })).toBeVisible();
    await expect(page.locator("th").filter({ hasText: "Response created at" })).toBeVisible();
    await expect(page.locator("th").filter({ hasText: "Response received at" })).toBeVisible();
  });
});
