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

test.describe("DAG Runs Tab", () => {
  test.setTimeout(60_000);

  test("navigate to DAG detail page and click Runs tab", async ({ dagRunsTabPage, successAndFailedRuns }) => {
    await dagRunsTabPage.navigateToDag(successAndFailedRuns.dagId);
    await dagRunsTabPage.clickRunsTab();

    await expect(dagRunsTabPage.page).toHaveURL(/.*\/dags\/[^/]+\/runs/);
  });

  test("verify run details display correctly", async ({ dagRunsTabPage, successAndFailedRuns }) => {
    await dagRunsTabPage.navigateToDag(successAndFailedRuns.dagId);
    await dagRunsTabPage.clickRunsTab();
    await dagRunsTabPage.verifyRunDetailsDisplay();
  });

  test("verify runs exist in table", async ({ dagRunsTabPage, successAndFailedRuns }) => {
    await dagRunsTabPage.navigateToDag(successAndFailedRuns.dagId);
    await dagRunsTabPage.clickRunsTab();
    await dagRunsTabPage.verifyRunsExist();
  });

  test("click on a run and verify run details page", async ({ dagRunsTabPage, successAndFailedRuns }) => {
    await dagRunsTabPage.navigateToDag(successAndFailedRuns.dagId);
    await dagRunsTabPage.clickRunsTab();
    await dagRunsTabPage.clickRunAndVerifyDetails();
  });

  test("filter runs by success state", async ({ dagRunsTabPage, successAndFailedRuns }) => {
    await dagRunsTabPage.navigateToDag(successAndFailedRuns.dagId);
    await dagRunsTabPage.clickRunsTab();
    await dagRunsTabPage.filterByState("success");
    await dagRunsTabPage.verifyFilteredByState("success");
  });

  test("filter runs by failed state", async ({ dagRunsTabPage, successAndFailedRuns }) => {
    await dagRunsTabPage.navigateToDag(successAndFailedRuns.dagId);
    await dagRunsTabPage.clickRunsTab();
    await dagRunsTabPage.filterByState("failed");
    await dagRunsTabPage.verifyFilteredByState("failed");
  });

  test("search for dag run by run ID pattern", async ({ dagRunsTabPage, successAndFailedRuns }) => {
    await dagRunsTabPage.navigateToDag(successAndFailedRuns.dagId);
    await dagRunsTabPage.clickRunsTab();
    // Extract the first 6 chars of the success run ID as a search pattern,
    // so the test is not coupled to a hardcoded fixture prefix.
    const searchPattern = successAndFailedRuns.successRun.runId.slice(0, 6);

    await dagRunsTabPage.searchByRunIdPattern(searchPattern);
    await dagRunsTabPage.verifySearchResults(searchPattern);
  });
});
