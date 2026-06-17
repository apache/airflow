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

test.describe("Dag Run Page", () => {
  test("verify HITL review modal opens from Dag run details", async ({ dagRunPage, pendingHITLRun }) => {
    test.slow();

    await dagRunPage.navigateToDagRun(pendingHITLRun.dagId, pendingHITLRun.runId);

    await expect(dagRunPage.requiredActionsButton).toBeVisible({ timeout: 60_000 });
    await dagRunPage.requiredActionsButton.click();

    await dagRunPage.hitlReviewModal.expectOpenWith(pendingHITLRun.runId);
  });

  test("verify HITL review modal opens from the required actions route", async ({
    dagRunPage,
    pendingHITLRun,
  }) => {
    test.slow();

    await dagRunPage.navigateToDagRunRequiredActions(pendingHITLRun.dagId, pendingHITLRun.runId);

    await dagRunPage.hitlReviewModal.expectOpenWith(pendingHITLRun.runId);
  });
});
