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
import { expect, type Locator, type Page } from "@playwright/test";
import { BasePage } from "tests/e2e/pages/BasePage";

import type { DAGRunResponse } from "openapi/requests/types.gen";

/**
 * Dags Page Object
 */
export class DagsPage extends BasePage {
  // Page URLs
  public static get dagsListUrl(): string {
    return "/dags";
  }

  // View toggle elements
  public readonly cardList: Locator;
  public readonly cardViewButton: Locator;
  // Search elements
  public readonly clearSearchButton: Locator;
  // Core page elements
  public readonly confirmButton: Locator;
  public readonly dagsTable: Locator;
  // Filter elements
  public readonly failedFilter: Locator;
  public readonly needsReviewFilter: Locator;
  // Pagination elements
  public readonly paginationNextButton: Locator;
  public readonly paginationPrevButton: Locator;
  public readonly queuedFilter: Locator;
  public readonly runningFilter: Locator;
  // Search elements
  public readonly searchInput: Locator;
  // Sort element (only visible in card view)
  public readonly sortSelect: Locator;
  public readonly stateElement: Locator;
  public readonly successFilter: Locator;
  public readonly tableList: Locator;
  public readonly tableViewButton: Locator;
  public readonly triggerButton: Locator;

  public constructor(page: Page) {
    super(page);
    this.dagsTable = page.locator('div:has([data-testid="dag-id"])');
    this.triggerButton = page.locator('button[aria-label="Trigger Dag"]:has-text("Trigger")');
    this.confirmButton = page.locator('button:has-text("Trigger")').nth(1);
    this.stateElement = page.locator('*:has-text("State") + *').first();
    this.paginationNextButton = page.locator('[data-testid="next"]');
    this.paginationPrevButton = page.locator('[data-testid="prev"]');

    // View toggle elements
    this.cardViewButton = page.getByRole("button", { name: /card/i });
    this.tableViewButton = page.getByRole("button", { name: /table/i });
    this.cardList = page.locator('[data-testid="card-list"]');
    this.tableList = page.locator('[data-testid="table-list"]');

    // Search elements
    this.searchInput = page.locator('[data-testid="search-dags"]');
    this.clearSearchButton = page.locator('[data-testid="clear-search"]');

    // Filter elements
    this.failedFilter = page.locator('[data-testid="dags-failed-filter"]');
    this.queuedFilter = page.locator('[data-testid="dags-queued-filter"]');
    this.runningFilter = page.locator('[data-testid="dags-running-filter"]');
    this.successFilter = page.locator('[data-testid="dags-success-filter"]');
    this.needsReviewFilter = page.locator('[data-testid="dags-needs-review-filter"]');

    // Sort element
    this.sortSelect = page.locator('[data-testid="sort-by-select"]');
  }

  // URL builders for dynamic paths
  public static getDagDetailUrl(dagName: string): string {
    return `/dags/${dagName}`;
  }

  public static getDagRunDetailsUrl(dagName: string, dagRunId: string): string {
    return `/dags/${dagName}/runs/${dagRunId}/details`;
  }

  /**
   * Clear the search input
   */
  public async clearSearch(): Promise<void> {
    // Clear the input by selecting all and deleting
    await this.searchInput.clear();
    // Wait for the search to update
    await this.page.waitForTimeout(500);
    await this.waitForPageLoad();
  }

  /**
   * Click next page button
   */
  public async clickNextPage(): Promise<void> {
    await this.paginationNextButton.click();
    await this.waitForDagList();
  }

  /**
   * Click previous page button
   */
  public async clickPrevPage(): Promise<void> {
    await this.paginationPrevButton.click();
    await this.waitForDagList();
  }

  /**
   * Click sort select (only works in card view)
   */
  public async clickSortSelect(): Promise<void> {
    await this.sortSelect.click();
  }

  /**
   * Filter Dags by status
   */
  public async filterByStatus(
    status: "failed" | "needs_review" | "queued" | "running" | "success",
  ): Promise<void> {
    const filterMap = {
      failed: this.failedFilter,
      needs_review: this.needsReviewFilter,
      queued: this.queuedFilter,
      running: this.runningFilter,
      success: this.successFilter,
    };

    await filterMap[status].click();
    await this.waitForPageLoad();
  }

  /**
   * Get all Dag links from the list
   */
  public async getDagLinks(): Promise<Array<string>> {
    // Select all links that point to specific Dags (href starts with /dags/)
    const links = await this.page.locator('a[href^="/dags/"]').all();
    const hrefs: Array<string> = [];

    for (const link of links) {
      const href = await link.getAttribute("href");

      // Only include links that match /dags/{dag_id} pattern (not /dags or /dags/{id}/runs/...)
      if (href !== null && href !== "" && /^\/dags\/[^/]+$/.exec(href) !== null) {
        hrefs.push(href);
      }
    }

    return hrefs;
  }

  /**
   * Get all Dag names from the current page
   */
  public async getDagNames(): Promise<Array<string>> {
    // Select all Dag links using href pattern
    const dagLinks = this.page.locator('a[href^="/dags/"]');

    // Wait for page to load - check for either Dag links or "No Dag found" message
    await Promise.race([
      dagLinks
        .first()
        .waitFor({ state: "visible", timeout: 10_000 })
        .catch(() => {
          // Ignore error, we're racing with "No Dag found" message
        }),
      this.page
        .locator("text=No Dag found")
        .waitFor({ state: "visible", timeout: 10_000 })
        .catch(() => {
          // Ignore error, we're racing with Dag links
        }),
    ]);

    const links = await dagLinks.all();
    const names: Array<string> = [];

    for (const link of links) {
      const href = await link.getAttribute("href");
      const text = await link.textContent();

      // Only include links that match /dags/{dag_id} pattern
      if (
        href !== null &&
        href !== "" &&
        /^\/dags\/[^/]+$/.exec(href) !== null &&
        text !== null &&
        text !== ""
      ) {
        names.push(text.trim());
      }
    }

    return names.filter((name) => name !== "");
  }

  /**
   * Get the count of Dags displayed in the list
   * Works for both card view and table view by counting Dag links with href pattern
   */
  public async getDagsCount(): Promise<number> {
    // Select all links that point to specific Dags
    const dagLinks = this.page.locator('a[href^="/dags/"]');

    // Wait for page to load - check for either Dag links or "No Dag found" message
    await Promise.race([
      dagLinks
        .first()
        .waitFor({ state: "visible", timeout: 10_000 })
        .catch(() => {
          // Ignore error, we're racing with "No Dag found" message
        }),
      this.page
        .locator("text=No Dag found")
        .waitFor({ state: "visible", timeout: 10_000 })
        .catch(() => {
          // Ignore error, we're racing with Dag links
        }),
    ]);

    const links = await dagLinks.all();
    let count = 0;

    for (const link of links) {
      const href = await link.getAttribute("href");

      // Only count links that match /dags/{dag_id} pattern
      if (href !== null && href !== "" && /^\/dags\/[^/]+$/.exec(href) !== null) {
        count++;
      }
    }

    return count;
  }

  /**
   * Navigate to Dags list page
   */
  public async navigate(): Promise<void> {
    await this.navigateTo(DagsPage.dagsListUrl);
  }

  /**
   * Navigate to Dag detail page
   */
  public async navigateToDagDetail(dagName: string): Promise<void> {
    await this.navigateTo(DagsPage.getDagDetailUrl(dagName));
  }

  /**
   * Search for a Dag by name
   */
  public async searchDag(searchTerm: string): Promise<void> {
    await this.searchInput.fill(searchTerm);
    await this.waitForPageLoad();
  }

  /**
   * Toggle to card view
   */
  public async switchToCardView(): Promise<void> {
    await this.cardViewButton.click();
    await this.cardList.first().waitFor({ state: "visible", timeout: 10_000 });
  }

  /**
   * Toggle to table view
   */
  public async switchToTableView(): Promise<void> {
    await this.tableViewButton.click();
    await this.tableList.waitFor({ state: "visible", timeout: 10_000 });
  }

  /**
   * Trigger a Dag run
   */
  public async triggerDag(dagName: string): Promise<string | null> {
    await this.navigateToDagDetail(dagName);
    await expect(this.triggerButton).toBeVisible({ timeout: 10_000 });
    await this.triggerButton.click();
    const dagRunId = await this.handleTriggerDialog();

    return dagRunId;
  }

  /**
   * Verify card view is displayed
   */
  public async verifyCardViewVisible(): Promise<boolean> {
    return await this.cardList.first().isVisible();
  }

  /**
   * Navigate to details tab and verify Dag details are displayed correctly
   */
  public async verifyDagDetails(dagName: string): Promise<void> {
    await this.navigateToDagDetail(dagName);

    const detailsTab = this.page.locator('a[href$="/details"]');

    await expect(detailsTab).toBeVisible();
    await detailsTab.click();

    // Verify the details table is present
    const detailsTable = this.page.locator('[data-testid="dag-details-table"]');

    await expect(detailsTable).toBeVisible();

    // Verify all metadata fields are present
    await expect(this.page.locator('[data-testid="dag-id-row"]')).toBeVisible();
    await expect(this.page.locator('[data-testid="description-row"]')).toBeVisible();
    await expect(this.page.locator('[data-testid="timezone-row"]')).toBeVisible();
    await expect(this.page.locator('[data-testid="file-location-row"]')).toBeVisible();
    await expect(this.page.locator('[data-testid="last-parsed-row"]')).toBeVisible();
    await expect(this.page.locator('[data-testid="last-parse-duration-row"]')).toBeVisible();
    await expect(this.page.locator('[data-testid="latest-dag-version-row"]')).toBeVisible();
    await expect(this.page.locator('[data-testid="start-date-row"]')).toBeVisible();
    await expect(this.page.locator('[data-testid="end-date-row"]')).toBeVisible();
    await expect(this.page.locator('[data-testid="last-expired-row"]')).toBeVisible();
    await expect(this.page.locator('[data-testid="has-task-concurrency-limits-row"]')).toBeVisible();
    await expect(this.page.locator('[data-testid="dag-run-timeout-row"]')).toBeVisible();
    await expect(this.page.locator('[data-testid="max-active-runs-row"]')).toBeVisible();
    await expect(this.page.locator('[data-testid="max-active-tasks-row"]')).toBeVisible();
    await expect(this.page.locator('[data-testid="max-consecutive-failed-dag-runs-row"]')).toBeVisible();
    await expect(this.page.locator('[data-testid="catchup-row"]')).toBeVisible();
    await expect(this.page.locator('[data-testid="default-args-row"]')).toBeVisible();
    await expect(this.page.locator('[data-testid="params-row"]')).toBeVisible();
  }

  /**
   * Verify that a specific Dag exists in the list
   */
  public async verifyDagExists(dagId: string): Promise<boolean> {
    const dagLink = this.page.locator(`a[href="/dags/${dagId}"]`);

    return await dagLink.isVisible();
  }

  public async verifyDagRunStatus(dagName: string, dagRunId: string | null): Promise<void> {
    // eslint-disable-next-line @typescript-eslint/no-unnecessary-condition
    if (dagRunId === null || dagRunId === undefined || dagRunId === "") {
      return;
    }

    await this.page.goto(DagsPage.getDagRunDetailsUrl(dagName, dagRunId), {
      timeout: 15_000,
      waitUntil: "domcontentloaded",
    });

    await this.page.waitForTimeout(2000);

    const maxWaitTime = 5 * 60 * 1000;
    const checkInterval = 10_000;
    const startTime = Date.now();

    while (Date.now() - startTime < maxWaitTime) {
      const currentStatus = await this.getCurrentDagRunStatus();

      if (currentStatus === "success") {
        return;
      } else if (currentStatus === "failed") {
        throw new Error(`Dag run failed: ${dagRunId}`);
      }

      await this.page.waitForTimeout(checkInterval);

      await this.page.reload({ waitUntil: "domcontentloaded" });

      await this.page.waitForTimeout(2000);
    }

    throw new Error(`Dag run did not complete within 5 minutes: ${dagRunId}`);
  }

  /**
   * Verify that the Dags list/table is visible on the page
   */
  public async verifyDagsListVisible(): Promise<void> {
    await this.dagsTable.first().waitFor({ state: "visible", timeout: 10_000 });
  }

  /**
   * Verify table view is displayed
   */
  public async verifyTableViewVisible(): Promise<boolean> {
    return await this.tableList.isVisible();
  }

  private async getCurrentDagRunStatus(): Promise<string> {
    try {
      const statusText = await this.stateElement.textContent().catch(() => "");
      const status = statusText?.trim() ?? "";

      switch (status) {
        case "Failed":
          return "failed";
        case "Queued":
          return "queued";
        case "Running":
          return "running";
        case "Success":
          return "success";
        default:
          return "unknown";
      }
    } catch {
      return "unknown";
    }
  }

  private async handleTriggerDialog(): Promise<string | null> {
    await this.page.waitForTimeout(1000);

    const responsePromise = this.page

      .waitForResponse(
        (response) => {
          const url = response.url();

          const method = response.request().method();

          return (
            method === "POST" && Boolean(url.includes("dagRuns")) && Boolean(!url.includes("hitlDetails"))
          );
        },
        { timeout: 10_000 },
      )

      .catch(() => undefined);

    await expect(this.confirmButton).toBeVisible({ timeout: 8000 });

    await this.page.waitForTimeout(2000);
    await this.confirmButton.click({ force: true });

    const apiResponse = await responsePromise;

    if (apiResponse) {
      try {
        const responseBody = await apiResponse.text();
        const responseJson = JSON.parse(responseBody) as DAGRunResponse;

        if (Boolean(responseJson.dag_run_id)) {
          return responseJson.dag_run_id;
        }
      } catch {
        // Response parsing failed
      }
    }

    // eslint-disable-next-line unicorn/no-null
    return null;
  }

  /**
   * Wait for DAG list to be rendered
   */
  private async waitForDagList(): Promise<void> {
    await expect(this.page.locator('[data-testid="dag-id"]').first()).toBeVisible({
      timeout: 10_000,
    });
  }
}
