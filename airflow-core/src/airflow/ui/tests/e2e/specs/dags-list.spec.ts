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
import { apiDeleteDagRun, waitForDagRunStatus } from "tests/e2e/utils/api/dag-runs";

test.describe("Dag Trigger Workflow", () => {
  const testDagId = testConfig.testDag.id;

  test("should successfully trigger a Dag run", async ({
    authenticatedRequest,
    dagReady: _ready,
    dagsPage,
    page,
  }) => {
    test.slow();

    const dagRunId = await dagsPage.triggerDag(testDagId);

    expect(dagRunId).toBeTruthy();

    if (dagRunId !== null) {
      await waitForDagRunStatus(authenticatedRequest, {
        dagId: testDagId,
        expectedState: "success",
        runId: dagRunId,
        timeout: 120_000,
      });

      await page.goto(`/dags/${testDagId}/runs/${dagRunId}`);
      const stateBadge = page.getByTestId("state-badge").first();

      await expect(stateBadge).toContainText("Success", { timeout: 30_000 });

      await apiDeleteDagRun(authenticatedRequest, testDagId, dagRunId).catch(() => undefined);
    }
  });
});

test.describe("Dag Details Tab", () => {
  const testDagId = testConfig.testDag.id;

  test("should successfully verify details tab", async ({ dagReady: _ready, dagsPage }) => {
    test.slow();
    await dagsPage.navigateToDagDetails(testDagId);
  });
});

test.describe("Dags List Display", () => {
  // dagReady is triggered once per worker via beforeEach.
  // eslint-disable-next-line @typescript-eslint/no-empty-function -- triggers worker-scoped data fixture
  test.beforeEach(async ({ dagReady: _ready }) => {});

  test("should display Dags list after successful login", async ({ dagsPage }) => {
    test.slow();
    await dagsPage.navigate();
    await dagsPage.waitForDagList();

    const dagsCount = await dagsPage.getDagsCount();

    expect(dagsCount).toBeGreaterThan(0);
  });

  test("should display Dag links correctly", async ({ dagsPage }) => {
    test.slow();
    await dagsPage.navigate();
    await dagsPage.waitForDagList();

    const dagLinks = await dagsPage.getDagLinks();

    expect(dagLinks.length).toBeGreaterThan(0);

    for (const link of dagLinks) {
      expect(link).toMatch(/\/dags\/.+/);
    }
  });

  test("should navigate from the list to a Dag", async ({ dagsPage, page }) => {
    test.slow();
    const testDagId = testConfig.testDag.id;

    await dagsPage.navigate();
    await dagsPage.waitForDagList();
    const dagLink = dagsPage.getDagLink(testDagId);

    await expect(dagLink).toBeVisible();
    await dagLink.click();
    await expect(page).toHaveURL(new RegExp(`/dags/${testDagId}`));
    await expect(
      page
        .getByRole("heading", { name: testDagId })
        .or(page.locator(`[data-testid="dag-name"]:has-text("${testDagId}")`))
        .first(),
    ).toBeVisible();
  });

  test("verify HITL review modal opens from the needs review badge in table view", async ({
    dagsPage,
    pendingHITLRun,
  }) => {
    test.slow();

    await dagsPage.navigate();
    await dagsPage.waitForDagList();
    await dagsPage.switchToTableView();

    await dagsPage.filterByStatus("needs_review");

    const needsReviewBadge = await dagsPage.getDagNeedsReviewBadgeOnTable(pendingHITLRun.dagId);

    await expect(needsReviewBadge).toBeVisible({ timeout: 30_000 });
    await needsReviewBadge.click();

    await dagsPage.hitlReviewModal.expectOpenWith(pendingHITLRun.dagId);
  });

  test("verify HITL review modal opens from the needs review badge in card view", async ({
    dagsPage,
    pendingHITLRun,
  }) => {
    test.slow();

    await dagsPage.navigate();
    await dagsPage.waitForDagList();
    await dagsPage.switchToCardView();

    await dagsPage.filterByStatus("needs_review");

    const needsReviewBadge = await dagsPage.getDagNeedsReviewBadgeOnCard(pendingHITLRun.dagId);

    await expect(needsReviewBadge).toBeVisible({ timeout: 30_000 });
    await needsReviewBadge.click();

    await dagsPage.hitlReviewModal.expectOpenWith(pendingHITLRun.dagId);
  });
});

test.describe("Dags View Toggle", () => {
  test("should toggle between card view and table view", async ({ dagsPage }) => {
    test.slow();
    await dagsPage.navigate();
    await dagsPage.waitForDagList();

    await dagsPage.switchToCardView();
    await dagsPage.waitForCardView();

    const cardViewDagsCount = await dagsPage.getDagsCount();

    expect(cardViewDagsCount).toBeGreaterThan(0);

    await dagsPage.switchToTableView();
    await dagsPage.waitForTableView();

    const tableViewDagsCount = await dagsPage.getDagsCount();

    expect(tableViewDagsCount).toBeGreaterThan(0);
  });
});

test.describe("Dags Search", () => {
  const testDagId = testConfig.testDag.id;

  test("should search for a Dag by name", async ({ dagsPage }) => {
    test.slow();
    await dagsPage.navigate();
    await dagsPage.waitForDagList();

    const initialCount = await dagsPage.getDagsCount();

    expect(initialCount).toBeGreaterThan(0);

    await dagsPage.searchDag(testDagId);
    await expect(dagsPage.getDagLink(testDagId)).toBeVisible();
    await dagsPage.clearSearch();

    await dagsPage.waitForDagList();

    await expect
      .poll(async () => dagsPage.getDagsCount(), {
        message: "Waiting for Dags count to restore after clearing search",
      })
      .toBe(initialCount);
  });

  test("should synchronize combined search and filters with browser history", async ({
    dagsPage,
    page,
    pendingHITLRun,
  }) => {
    test.slow();
    const pendingDagId = pendingHITLRun.dagId;

    await dagsPage.navigate();
    await dagsPage.searchDag(pendingDagId);

    await expect.poll(() => new URL(page.url()).searchParams.get("name_pattern")).toBe(pendingDagId);

    await dagsPage.filterByStatus("needs_review", { dag_display_name_prefix_pattern: pendingDagId });
    await expect.poll(() => new URL(page.url()).searchParams.get("needs_review")).toBe("true");
    await expect(dagsPage.searchInput).toHaveValue(pendingDagId);
    await expect(dagsPage.getDagLink(pendingDagId)).toBeVisible();

    await page.goBack();
    await expect.poll(() => new URL(page.url()).searchParams.get("needs_review")).toBeNull();
    await expect(dagsPage.searchInput).toHaveValue(pendingDagId);
    await expect(dagsPage.getDagLink(pendingDagId)).toBeVisible();
    await expect(page.getByTestId("hub-edit-needsReview")).toBeHidden();

    await page.goForward();
    await expect.poll(() => new URL(page.url()).searchParams.get("needs_review")).toBe("true");
    await expect(page.getByTestId("hub-edit-needsReview")).toBeVisible();
    await expect(dagsPage.getDagLink(pendingDagId)).toBeVisible();

    await dagsPage.clearFilters();
    await expect.poll(() => new URL(page.url()).searchParams.get("needs_review")).toBeNull();
    await expect(dagsPage.searchInput).toHaveValue(pendingDagId);
  });

  test("should hydrate a multi-filter deep link and remove one active chip", async ({
    dagsPage,
    page,
    pendingHITLRun,
  }) => {
    test.slow();
    const pendingDagId = pendingHITLRun.dagId;
    const responsePromise = dagsPage.waitForDagsResponse({
      dag_display_name_prefix_pattern: pendingDagId,
      has_pending_actions: "true",
      paused: "false",
    });

    await page.goto(`/dags?name_pattern=${encodeURIComponent(pendingDagId)}&needs_review=true&paused=false`);
    await responsePromise;

    await expect(dagsPage.searchInput).toHaveValue(pendingDagId);
    await expect(page.getByTestId("hub-edit-needsReview")).toBeVisible();
    await expect(page.getByTestId("hub-edit-paused")).toBeVisible();

    await dagsPage.removeFilter("paused", {
      dag_display_name_prefix_pattern: pendingDagId,
      has_pending_actions: "true",
      paused: null,
    });
    await expect
      .poll(() => ["all", null].includes(new URL(page.url()).searchParams.get("paused")))
      .toBe(true);
    await expect(page.getByTestId("hub-edit-paused")).toBeHidden();
  });

  test("should show the no-result state for a missing Dag", async ({ dagsPage, page }) => {
    await dagsPage.navigate();
    await dagsPage.searchDag("dag-that-does-not-exist-69728");

    await expect(page.getByText(/no dag/i)).toBeVisible();
  });
});

test.describe("Dags Status Filtering", () => {
  test("should filter Dags by run state", async ({ dagsPage }) => {
    test.slow();
    await dagsPage.navigate();
    await dagsPage.waitForDagList();

    await dagsPage.filterByStatus("success");
    await dagsPage.waitForDagList();

    await dagsPage.filterByStatus("failed");
    await dagsPage.waitForDagList();
  });

  test("should support keyboard dismissal and restore focus", async ({ dagsPage }) => {
    await dagsPage.navigate();
    await dagsPage.openFilters();

    await expect(dagsPage.lastRunStateFilter.getByRole("combobox")).toBeFocused();
    await dagsPage.closeFilters();
  });

  test("should use a non-overflowing filter drawer on a narrow viewport", async ({ dagsPage, page }) => {
    await page.setViewportSize({ height: 812, width: 375 });
    const responsePromise = dagsPage.waitForDagsResponse({ paused: "false" });

    await page.goto("/dags?paused=false");
    await responsePromise;
    await expect(dagsPage.filterTrigger).toHaveAccessibleName(/1 active filter/i);
    await dagsPage.openFilters();

    const drawer = page.getByRole("dialog", { name: "Filter Dags" });
    const bounds = await drawer.boundingBox();

    const hasHorizontalOverflow = await page.evaluate(
      () => document.documentElement.scrollWidth > document.documentElement.clientWidth,
    );

    expect(hasHorizontalOverflow).toBe(false);
    expect(bounds).not.toBeNull();
    expect(bounds?.x).toBeGreaterThanOrEqual(0);
    expect((bounds?.x ?? 0) + (bounds?.width ?? 0)).toBeLessThanOrEqual(375);
    await expect(dagsPage.lastRunStateFilter).toBeVisible();

    await dagsPage.clearFilters();
    await expect(dagsPage.filterTrigger).toBeFocused();
  });
});
