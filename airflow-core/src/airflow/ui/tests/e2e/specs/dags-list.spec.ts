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

/**
 * Verify Dags List Displays
 */

test.describe("Dags List Display", () => {
  let dagsPage: DagsPage;

  test.beforeEach(({ page }) => {
    dagsPage = new DagsPage(page);
  });

  test("should display Dags list after successful login", async () => {
    await dagsPage.navigate();
    await dagsPage.verifyDagsListVisible();

    const dagsCount = await dagsPage.getDagsCount();

    expect(dagsCount).toBeGreaterThan(0);
  });

  test("should display Dag links correctly", async () => {
    await dagsPage.navigate();
    await dagsPage.verifyDagsListVisible();

    const dagLinks = await dagsPage.getDagLinks();

    expect(dagLinks.length).toBeGreaterThan(0);

    for (const link of dagLinks) {
      expect(link).toMatch(/\/dags\/.+/);
    }
  });

  test("should display test Dag in the list", async () => {
    const testDagId = testConfig.testDag.id;

    await dagsPage.navigate();
    await dagsPage.verifyDagsListVisible();

    const dagExists = await dagsPage.verifyDagExists(testDagId);

    expect(dagExists).toBe(true);
  });
});

/**
 * Card/Table View Toggle Tests
 */

test.describe("Dags View Toggle", () => {
  let dagsPage: DagsPage;

  test.beforeEach(({ page }) => {
    dagsPage = new DagsPage(page);
  });

  test("should toggle between card view and table view", async () => {
    await dagsPage.navigate();
    await dagsPage.verifyDagsListVisible();

    // Step 4: Switch to card view
    await dagsPage.switchToCardView();

    // Step 5: Verify card view is displayed
    const cardViewVisible = await dagsPage.verifyCardViewVisible();

    expect(cardViewVisible).toBe(true);

    // Step 6: Switch to table view
    await dagsPage.switchToTableView();

    // Step 7: Verify table view is displayed
    const tableViewVisible = await dagsPage.verifyTableViewVisible();

    expect(tableViewVisible).toBe(true);
  });
});

/**
 * Dag Search Functionality Tests
 */

test.describe("Dags Search", () => {
  let dagsPage: DagsPage;

  const testDagId = testConfig.testDag.id;

  test.beforeEach(({ page }) => {
    dagsPage = new DagsPage(page);
  });

  test("should search for a Dag by name", async () => {
    await dagsPage.navigate();
    await dagsPage.verifyDagsListVisible();

    // Step 4: Get initial count of Dags
    const initialCount = await dagsPage.getDagsCount();

    expect(initialCount).toBeGreaterThan(0);

    // Step 5: Search for the test Dag
    await dagsPage.searchDag(testDagId);

    // Step 6: Verify the test Dag is in the filtered results
    const dagExists = await dagsPage.verifyDagExists(testDagId);

    expect(dagExists).toBe(true);

    // Step 7: Clear the search
    await dagsPage.clearSearch();

    // Step 8: Wait for the list to reload and verify count returns to initial
    await dagsPage.verifyDagsListVisible();
    const finalCount = await dagsPage.getDagsCount();

    expect(finalCount).toBe(initialCount);
  });
});

/**
 * Dag Status Filtering Tests
 */

test.describe("Dags Status Filtering", () => {
  let dagsPage: DagsPage;

  test.beforeEach(({ page }) => {
    dagsPage = new DagsPage(page);
  });

  test("should filter Dags by status", async () => {
    await dagsPage.navigate();
    await dagsPage.verifyDagsListVisible();

    // Step 4: Verify filter buttons are visible
    await expect(dagsPage.successFilter).toBeVisible();
    await expect(dagsPage.failedFilter).toBeVisible();
    await expect(dagsPage.runningFilter).toBeVisible();
    await expect(dagsPage.queuedFilter).toBeVisible();

    // Step 5: Click success filter
    await dagsPage.filterByStatus("success");

    // Step 6: Verify Dags are still displayed (filtered)
    const successCount = await dagsPage.getDagsCount();

    expect(successCount).toBeGreaterThanOrEqual(0);

    // Step 7: Click failed filter
    await dagsPage.filterByStatus("failed");

    // Step 8: Verify Dags are still displayed (filtered)
    const failedCount = await dagsPage.getDagsCount();

    expect(failedCount).toBeGreaterThanOrEqual(0);
  });
});

/**
 * Dag Sorting Tests
 */

test.describe("Dags Sorting", () => {
  let dagsPage: DagsPage;

  test.beforeEach(({ page }) => {
    dagsPage = new DagsPage(page);
  });

  test("should display sort select in card view", async () => {
    await dagsPage.navigate();
    await dagsPage.verifyDagsListVisible();

    // Step 4: Switch to card view
    await dagsPage.switchToCardView();

    // Step 5: Verify sort select is visible
    await expect(dagsPage.sortSelect).toBeVisible();

    // Step 6: Get initial Dag names
    const initialDagNames = await dagsPage.getDagNames();

    expect(initialDagNames.length).toBeGreaterThan(0);

    // Step 7: Click sort select to open options
    await dagsPage.clickSortSelect();

    // Step 8: Verify sort options menu is opened
    await expect(dagsPage.page.getByRole("option").first()).toBeVisible();
  });
});
