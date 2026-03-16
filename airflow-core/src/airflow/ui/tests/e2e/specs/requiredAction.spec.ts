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
import { AUTH_FILE, testConfig } from "playwright.config";

import { RequiredActionsPage } from "../pages/RequiredActionsPage";

const hitlDagId = testConfig.testDag.hitlId;

test.describe("Verify Required Action page", () => {
  test.describe.configure({ mode: "serial" });

  test.beforeAll(async ({ browser }) => {
    test.setTimeout(400_000);

    const context = await browser.newContext({ storageState: AUTH_FILE });

    const page = await context.newPage();
    const requiredActionsPage = new RequiredActionsPage(page);

    await requiredActionsPage.runHITLFlowWithApproval(hitlDagId);
    await requiredActionsPage.runHITLFlowWithRejection(hitlDagId);

    await context.close();
  });

  test("Verify the actions list/table is displayed (or empty state if none)", async ({ page }) => {
    const browsePage = new RequiredActionsPage(page);

    await browsePage.navigateToRequiredActionsPage();

    await expect(browsePage.actionsTable.or(browsePage.emptyStateMessage)).toBeVisible();

    if (await browsePage.actionsTable.isVisible()) {
      await expect(page.locator("th").filter({ hasText: "Dag ID" })).toBeVisible();
      await expect(page.locator("th").filter({ hasText: "Task ID" })).toBeVisible();
      await expect(page.locator("th").filter({ hasText: "Dag Run ID" })).toBeVisible();
      await expect(page.locator("th").filter({ hasText: "Response created at" })).toBeVisible();
      await expect(page.locator("th").filter({ hasText: "Response received at" })).toBeVisible();
    } else {
      await expect(browsePage.emptyStateMessage).toBeVisible();
    }
  });
});
