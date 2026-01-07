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

test.describe("Dags Pagination", () => {
  let dagsPage: DagsPage;

  test.beforeEach(({ page }) => {
    dagsPage = new DagsPage(page);
  });

  test("should verify pagination works on the Dags list page", async () => {
    await dagsPage.navigate();

    await expect(dagsPage.paginationNextButton).toBeVisible();
    await expect(dagsPage.paginationPrevButton).toBeVisible();

    const initialDagNames = await dagsPage.getDagNames();

    expect(initialDagNames.length).toBeGreaterThan(0);

    await dagsPage.clickNextPage();

    const dagNamesAfterNext = await dagsPage.getDagNames();

    expect(dagNamesAfterNext.length).toBeGreaterThan(0);
    expect(dagNamesAfterNext).not.toEqual(initialDagNames);

    await dagsPage.clickPrevPage();

    const dagNamesAfterPrev = await dagsPage.getDagNames();

    expect(dagNamesAfterPrev).toEqual(initialDagNames);
  });
});

test.describe("Dag Trigger Workflow", () => {
  let dagsPage: DagsPage;
  const testDagId = testConfig.testDag.id;

  test.beforeEach(({ page }) => {
    dagsPage = new DagsPage(page);
  });

  test("should successfully trigger a Dag run", async () => {
    test.setTimeout(7 * 60 * 1000);

    const dagRunId = await dagsPage.triggerDag(testDagId);

    if (Boolean(dagRunId)) {
      await dagsPage.verifyDagRunStatus(testDagId, dagRunId);
    }
  });
});

test.describe("Dag Details Tab", () => {
  let dagsPage: DagsPage;

  const testDagId = testConfig.testDag.id;

  test.beforeEach(({ page }) => {
    dagsPage = new DagsPage(page);
  });

  test("should successfully verify details tab", async () => {
    await dagsPage.verifyDagDetails(testDagId);
  });
});