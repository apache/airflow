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

  public readonly cardViewButton: Locator;
  public readonly confirmButton: Locator;
  public readonly failedFilter: Locator;
  public readonly needsReviewFilter: Locator;
  public readonly operatorFilter: Locator;
  public readonly queuedFilter: Locator;
  public readonly retriesFilter: Locator;
  public readonly runningFilter: Locator;
  public readonly searchBox: Locator;
  public readonly searchInput: Locator;
  public readonly stateElement: Locator;
  public readonly successFilter: Locator;
  public readonly tableViewButton: Locator;
  public readonly triggerButton: Locator;
  public readonly triggerRuleFilter: Locator;

  public get taskRows(): Locator {
    return this.page.locator('[data-testid="table-list"] > tbody > tr');
  }

  public constructor(page: Page) {
    super(page);
    this.triggerButton = page.locator('button[aria-label="Trigger Dag"]:has-text("Trigger")');
    // Use .last() instead of .nth(1) — when the modal opens, the confirm button
    // is the last "Trigger" button in the DOM regardless of whether the main
    // page trigger button has visible text or is icon-only.
    this.confirmButton = page.locator('button:has-text("Trigger")').last();
    this.stateElement = page.locator('*:has-text("State") + *').first();
    this.searchBox = page.getByRole("textbox", { name: /search/i });
    this.searchInput = page.getByPlaceholder("Search DAGs");
    this.operatorFilter = page.getByRole("combobox").filter({ hasText: /operator/i });
    this.triggerRuleFilter = page.getByRole("combobox").filter({ hasText: /trigger/i });
    this.retriesFilter = page.getByRole("combobox").filter({ hasText: /retr/i });
    // View toggle buttons
    this.cardViewButton = page.locator('button[aria-label="Show card view"]');
    this.tableViewButton = page.locator('button[aria-label="Show table view"]');
    // Status filter buttons
    this.successFilter = page.locator('button:has-text("Success")');
    this.failedFilter = page.locator('button:has-text("Failed")');
    this.runningFilter = page.locator('button:has-text("Running")');
    this.queuedFilter = page.locator('button:has-text("Queued")');
    this.needsReviewFilter = page.locator('button:has-text("Needs Review")');
  }

  // URL builders for dynamic paths
  public static getDagDetailUrl(dagName: string): string {
    return `/dags/${dagName}`;
  }

  public static getDagRunDetailsUrl(dagName: string, dagRunId: string): string {
    return `/dags/${dagName}/runs/${dagRunId}/details`;
  }

  /**
   * Clear the search input and wait for list to reset
   */
  public async clearSearch(): Promise<void> {
    await this.searchInput.clear();

    // Trigger blur to ensure the clear action is processed
    await this.searchInput.blur();

    // Small delay to allow React to process the state change
    await this.page.waitForTimeout(500);

    // Wait for the DAG list to be visible again
    await this.waitForDagList();
  }

  public async filterByOperator(operator: string): Promise<void> {
    await this.selectDropdownOption(this.operatorFilter, operator);
  }

  public async filterByRetries(retries: string): Promise<void> {
    await this.selectDropdownOption(this.retriesFilter, retries);
  }

  /**
   * Filter DAGs by status
   */
  public async filterByStatus(
    status: "failed" | "needs_review" | "queued" | "running" | "success",
  ): Promise<void> {
    const filterMap: Record<typeof status, Locator> = {
      failed: this.failedFilter,
      needs_review: this.needsReviewFilter,
      queued: this.queuedFilter,
      running: this.runningFilter,
      success: this.successFilter,
    };

    await filterMap[status].click();
    await this.page.waitForTimeout(500);
  }

  public async filterByTriggerRule(rule: string): Promise<void> {
    await this.selectDropdownOption(this.triggerRuleFilter, rule);
  }

  /**
   * Get all Dag links from the list
   */
  public async getDagLinks(): Promise<Array<string>> {
    await this.waitForDagList();

    // Check which view is active
    const cardList = this.page.locator('[data-testid="card-list"]');
    const isCardView = await cardList.isVisible();

    const links = isCardView
      ? await this.page.locator('[data-testid="dag-id"]').all()
      : await this.page.locator('[data-testid="table-list"] tbody tr td:nth-child(2) a').all();

    const hrefs: Array<string> = [];

    for (const link of links) {
      const href = await link.getAttribute("href");

      if (href !== null && href !== "") {
        hrefs.push(href);
      }
    }

    return hrefs;
  }

  /**
   * Get all Dag names from the current page
   */
  public async getDagNames(): Promise<Array<string>> {
    await this.waitForDagList();

    // Check which view is active
    const cardList = this.page.locator('[data-testid="card-list"]');
    const isCardView = await cardList.isVisible();

    const dagLinks = isCardView
      ? this.page.locator('[data-testid="dag-id"]')
      : this.page.locator('[data-testid="table-list"] tbody tr td:nth-child(2) a');

    const texts = await dagLinks.allTextContents();

    return texts.map((text) => text.trim()).filter((text) => text !== "");
  }

  /**
   * Get count of DAGs on current page
   */
  public async getDagsCount(): Promise<number> {
    await this.waitForDagList();

    // Check which view is active
    const cardList = this.page.locator('[data-testid="card-list"]');
    const isCardView = await cardList.isVisible();

    if (isCardView) {
      // Card view: count dag-id elements
      const dagCards = this.page.locator('[data-testid="dag-id"]');

      return dagCards.count();
    } else {
      // Table view: count table body rows
      const tableRows = this.page.locator('[data-testid="table-list"] tbody tr');

      return tableRows.count();
    }
  }

  public async getFilterOptions(filter: Locator): Promise<Array<string>> {
    await filter.click();
    await this.page.waitForTimeout(500);

    const controlsId = await filter.getAttribute("aria-controls");
    let options;

    if (controlsId === null) {
      const listbox = this.page.locator('div[role="listbox"]').first();

      await listbox.waitFor({ state: "visible", timeout: 5000 });
      options = listbox.locator('div[role="option"]');
    } else {
      options = this.page.locator(`[id="${controlsId}"] div[role="option"]`);
    }

    const count = await options.count();
    const dataValues: Array<string> = [];

    for (let i = 0; i < count; i++) {
      const value = await options.nth(i).getAttribute("data-value");

      if (value !== null && value.trim().length > 0) {
        dataValues.push(value);
      }
    }

    await this.page.keyboard.press("Escape");
    await this.page.waitForTimeout(300);

    return dataValues;
  }

  /**
   * Navigate to Dags list page
   */
  public async navigate(): Promise<void> {
    // Set up API listener before navigation
    const responsePromise = this.page
      .waitForResponse((resp) => resp.url().includes("/api/v2/dags") && resp.status() === 200, {
        timeout: 60_000,
      })
      .catch(() => {
        /* API might fail or timeout */
      });

    await this.navigateTo(DagsPage.dagsListUrl);

    // Wait for initial API response
    await responsePromise;

    // Give UI time to render the response
    await this.page.waitForTimeout(500);
  }

  /**
   * Navigate to Dag detail page
   */
  public async navigateToDagDetail(dagName: string): Promise<void> {
    await this.navigateTo(DagsPage.getDagDetailUrl(dagName));
  }

  public async navigateToDagTasks(dagId: string): Promise<void> {
    await this.page.goto(`/dags/${dagId}/tasks`);
    await this.page
      .locator("th")
      .filter({ hasText: /^Operator$/ })
      .first()
      .waitFor({ state: "visible", timeout: 30_000 });
  }

  /**
   * Search for a Dag by name
   */
  public async searchDag(searchTerm: string): Promise<void> {
    const currentNames = await this.getDagNames();

    const responsePromise = this.page
      .waitForResponse((resp) => resp.url().includes("/dags") && resp.status() === 200, {
        timeout: 30_000,
      })
      .catch(() => {
        /* API might be cached */
      });

    await this.searchInput.fill(searchTerm);

    await responsePromise;

    await expect
      .poll(
        async () => {
          const noDagFound = this.page.locator("text=/no dag/i");
          const isNoDagVisible = await noDagFound.isVisible().catch(() => false);

          if (isNoDagVisible) {
            return true;
          }

          const newNames = await this.getDagNames();

          return newNames.join(",") !== currentNames.join(",");
        },
        { message: "List did not update after search", timeout: 30_000 },
      )
      .toBe(true);

    await this.waitForDagList();
  }

  /**
   * Switch to card view
   */
  public async switchToCardView(): Promise<void> {
    // Wait for the button to be visible and enabled
    await expect(this.cardViewButton).toBeVisible({ timeout: 30_000 });
    await expect(this.cardViewButton).toBeEnabled({ timeout: 10_000 });
    await this.cardViewButton.click();
    // Wait for card view to be rendered
    await this.page.waitForTimeout(500);
    await this.verifyCardViewVisible();
  }

  /**
   * Switch to table view
   */
  public async switchToTableView(): Promise<void> {
    // Wait for the button to be visible and enabled
    await expect(this.tableViewButton).toBeVisible({ timeout: 30_000 });
    await expect(this.tableViewButton).toBeEnabled({ timeout: 10_000 });
    await this.tableViewButton.click();
    // Wait for table view to be rendered
    await this.page.waitForTimeout(500);
    await this.verifyTableViewVisible();
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
   * Verify card view is visible
   */
  public async verifyCardViewVisible(): Promise<boolean> {
    const cardList = this.page.locator('[data-testid="card-list"]');

    try {
      await cardList.waitFor({ state: "visible", timeout: 10_000 });

      return true;
    } catch {
      return false;
    }
  }

  /**
   * Navigate to details tab and verify Dag details are displayed correctly
   */
  public async verifyDagDetails(dagName: string): Promise<void> {
    // Navigate directly to the details URL
    await this.page.goto(`/dags/${dagName}/details`, { waitUntil: "domcontentloaded" });

    // Wait for page to load
    await this.page.waitForTimeout(1000);

    // Use getByRole to precisely target the heading element
    // This avoids "strict mode violation" from matching breadcrumbs, file paths, etc.
    await expect(this.page.getByRole("heading", { name: dagName })).toBeVisible({ timeout: 30_000 });
  }

  /**
   * Verify if a specific Dag exists in the list
   */
  public async verifyDagExists(dagId: string): Promise<boolean> {
    await this.waitForDagList();

    // Check which view is active
    const cardList = this.page.locator('[data-testid="card-list"]');
    const isCardView = await cardList.isVisible();

    const dagLink = isCardView
      ? this.page.locator(`[data-testid="dag-id"]:has-text("${dagId}")`)
      : this.page.locator(`[data-testid="table-list"] tbody tr td a:has-text("${dagId}")`);

    try {
      await dagLink.waitFor({ state: "visible", timeout: 10_000 });

      return true;
    } catch {
      return false;
    }
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

    const maxWaitTime = 7 * 60 * 1000;
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
   * Verify that the Dags list is visible
   */
  public async verifyDagsListVisible(): Promise<void> {
    await this.waitForDagList();
  }

  /**
   * Verify table view is visible
   */
  public async verifyTableViewVisible(): Promise<boolean> {
    const table = this.page.locator("table");

    try {
      await table.waitFor({ state: "visible", timeout: 10_000 });

      return true;
    } catch {
      return false;
    }
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

  private async selectDropdownOption(filter: Locator, value: string): Promise<void> {
    await filter.click();
    await this.page.locator(`div[role="option"][data-value="${value}"]`).dispatchEvent("click");
    await this.page.waitForTimeout(500);
  }

  /**
   * Wait for DAG list to be rendered
   */
  private async waitForDagList(): Promise<void> {
    // Define multiple possible UI states: Card View, Table View, or Empty State.
    // Use regex (/no dag/i) to handle case and singular/plural variations
    // (e.g. "No Dag found", "No Dags found", "NO DAG FOUND").
    const cardList = this.page.locator('[data-testid="card-list"]');
    const tableList = this.page.locator('[data-testid="table-list"]');
    const noDagFound = this.page.locator("text=/no dag/i");
    const fallbackTable = this.page.locator("table");

    // Wait for any of these elements to appear
    await expect(cardList.or(tableList).or(noDagFound).or(fallbackTable)).toBeVisible({
      timeout: 30_000,
    });

    // If empty state is shown, consider the list as successfully rendered
    if (await noDagFound.isVisible().catch(() => false)) {
      return;
    }

    // Wait for loading to complete (skeletons to disappear)
    const skeleton = this.page.locator('[data-testid="skeleton"]');

    await expect(skeleton).toHaveCount(0, { timeout: 30_000 });

    // Now wait for actual DAG content based on current view
    const isCardView = await cardList.isVisible().catch(() => false);

    if (isCardView) {
      // Card view: wait for dag-id elements
      const dagCards = this.page.locator('[data-testid="dag-id"]');

      await dagCards.first().waitFor({ state: "visible", timeout: 30_000 });
    } else {
      // Table view: prefer table-list testid, fallback to any <table> element
      const rowsInTableList = tableList.locator("tbody tr");

      if ((await rowsInTableList.count().catch(() => 0)) > 0) {
        await rowsInTableList.first().waitFor({ state: "visible", timeout: 30_000 });
      } else {
        const anyTableRows = fallbackTable.locator("tbody tr");

        await anyTableRows.first().waitFor({ state: "visible", timeout: 30_000 });
      }
    }
  }
}
