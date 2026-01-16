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
    test.setTimeout(120_000); // 2 minutes for slower browsers like Firefox
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

test.describe("Dags View Toggle", () => {
  let dagsPage: DagsPage;

  test.beforeEach(({ page }) => {
    dagsPage = new DagsPage(page);
  });

  test("should toggle between card view and table view", async () => {
    await dagsPage.navigate();
    await dagsPage.verifyDagsListVisible();

    await dagsPage.switchToCardView();

    const cardViewVisible = await dagsPage.verifyCardViewVisible();

    expect(cardViewVisible).toBe(true);

    const cardViewDagsCount = await dagsPage.getDagsCount();

    expect(cardViewDagsCount).toBeGreaterThan(0);

    await dagsPage.switchToTableView();

    const tableViewVisible = await dagsPage.verifyTableViewVisible();

    expect(tableViewVisible).toBe(true);

    const tableViewDagsCount = await dagsPage.getDagsCount();

    expect(tableViewDagsCount).toBeGreaterThan(0);
  });
});

test.describe("Dags Search", () => {
  let dagsPage: DagsPage;

  const testDagId = testConfig.testDag.id;

  test.beforeEach(({ page }) => {
    dagsPage = new DagsPage(page);
  });

  test("should search for a Dag by name", async () => {
    test.setTimeout(120_000); // 2 minutes for slower browsers like Firefox
    await dagsPage.navigate();
    await dagsPage.verifyDagsListVisible();

    const initialCount = await dagsPage.getDagsCount();

    expect(initialCount).toBeGreaterThan(0);

    await dagsPage.searchDag(testDagId);

    const dagExists = await dagsPage.verifyDagExists(testDagId);

    expect(dagExists).toBe(true);

    await dagsPage.clearSearch();

    await dagsPage.verifyDagsListVisible();
    const finalCount = await dagsPage.getDagsCount();

    expect(finalCount).toBe(initialCount);
  });
});

test.describe("Dags Status Filtering", () => {
  let dagsPage: DagsPage;

  test.beforeEach(({ page }) => {
    dagsPage = new DagsPage(page);
  });

  test("should display status filter buttons", async () => {
    test.setTimeout(120_000);
    await dagsPage.navigate();
    await dagsPage.verifyDagsListVisible();

    await expect(dagsPage.successFilter).toBeVisible();
    await expect(dagsPage.failedFilter).toBeVisible();
    await expect(dagsPage.runningFilter).toBeVisible();
    await expect(dagsPage.queuedFilter).toBeVisible();

    await dagsPage.filterByStatus("success");
    await dagsPage.verifyDagsListVisible();

    await dagsPage.filterByStatus("failed");
    await dagsPage.verifyDagsListVisible();
  });
});

test.describe("Dags Sorting", () => {
  let dagsPage: DagsPage;

  test.beforeEach(({ page }) => {
    dagsPage = new DagsPage(page);
  });

  test("should sort Dags by name in card view", async () => {
    await dagsPage.navigate();
    await dagsPage.verifyDagsListVisible();

    await dagsPage.switchToCardView();

    await expect(dagsPage.sortSelect).toBeVisible();

    const ascNames = await dagsPage.getDagNames();

    expect(ascNames.length).toBeGreaterThan(1);

    await dagsPage.clickSortSelect();

    await expect(dagsPage.page.getByRole("option").first()).toBeVisible();

    await dagsPage.page.getByRole("option", { name: "Sort by Display Name (Z-A)" }).click();

    await dagsPage.page.waitForTimeout(500);

    const descNames = await dagsPage.getDagNames();

    expect(descNames.length).toBeGreaterThan(1);

    const [firstName] = descNames;
    const lastName = descNames[descNames.length - 1];

    expect(firstName).toBeDefined();
    expect(lastName).toBeDefined();

    expect(firstName).not.toEqual(ascNames[0]);

    if (firstName !== undefined && firstName !== "" && lastName !== undefined && lastName !== "") {
      expect(firstName.localeCompare(lastName)).toBeGreaterThan(0);
    }
  });
});
