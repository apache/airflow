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
import { expect, type Locator, type Page, type Response } from "@playwright/test";
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
    this.triggerButton = page.getByTestId("trigger-dag-button");
    // Scoped to the dialog so we never accidentally click the page-level trigger.
    this.confirmButton = page.getByRole("dialog").getByRole("button", { name: "Trigger" });
    this.stateElement = page.getByTestId("dag-run-state");
    this.searchBox = page.getByRole("textbox", { name: /search/i });
    this.searchInput = page.getByPlaceholder("Search DAGs");
    this.operatorFilter = page.getByTestId("operator-filter");
    this.triggerRuleFilter = page.getByTestId("trigger-rule-filter");
    this.retriesFilter = page.getByTestId("retries-filter");
    // View toggle buttons
    this.cardViewButton = page.getByRole("button", { name: "Show card view" });
    this.tableViewButton = page.getByRole("button", { name: "Show table view" });
    // Status filter buttons
    this.successFilter = page.getByRole("button", { name: "Success" });
    this.failedFilter = page.getByRole("button", { name: "Failed" });
    this.runningFilter = page.getByRole("button", { name: "Running" });
    this.queuedFilter = page.getByRole("button", { name: "Queued" });
    // Uses testId because this button's text is driven by an i18n key.
    this.needsReviewFilter = page.getByTestId("dags-needs-review-filter");
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
    const responsePromise = this.page
      .waitForResponse((resp: Response) => resp.url().includes("/api/v2/dags") && resp.status() === 200, {
        timeout: 10_000,
      })
      .catch(() => {
        /* API response may be cached */
      });

    // Click the clear button instead of programmatically clearing the input.
    // The SearchBar component uses a 200ms debounce on keystroke changes,
    // but the clear button calls onChange("") directly, bypassing the debounce.
    await this.page.getByTestId("clear-search").click();
    await responsePromise;
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

    // Set up response listener before the click so we don't miss a fast response.
    const responsePromise = this.page
      .waitForResponse((resp: Response) => resp.url().includes("/api/v2/dags") && resp.status() === 200, {
        timeout: 10_000,
      })
      .catch(() => {
        // Some filters are applied client-side and don't trigger a network request.
      });

    await filterMap[status].click();
    await responsePromise;
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
      return this.page.locator('[data-testid="dag-id"]').count();
    }

    // Table view: count table body rows
    return this.page.locator('[data-testid="table-list"] tbody tr').count();
  }

  public async getFilterOptions(filter: Locator): Promise<Array<string>> {
    await filter.click();

    const controlsId = await filter.getAttribute("aria-controls");

    const dropdown =
      controlsId === null
        ? this.page.locator('div[role="listbox"]').first()
        : this.page.locator(`[id="${controlsId}"]`);

    await expect(dropdown).toBeVisible({ timeout: 5000 });
    const options = dropdown.locator('div[role="option"]');

    const count = await options.count();
    const dataValues: Array<string> = [];

    for (let i = 0; i < count; i++) {
      const value = await options.nth(i).getAttribute("data-value");

      if (value !== null && value.trim().length > 0) {
        dataValues.push(value);
      }
    }

    await this.page.keyboard.press("Escape");
    // Wait for the dropdown to close. On WebKit, the listbox may stay in the DOM
    // with data-state="closed", while on other browsers it may be removed entirely.
    await expect(dropdown)
      .toBeHidden({ timeout: 5000 })
      .catch(() => expect(dropdown).toHaveAttribute("data-state", "closed", { timeout: 1000 }));

    return dataValues;
  }

  /**
   * Navigate to Dags list page
   */
  public async navigate(): Promise<void> {
    // Set up API listener before navigation
    const responsePromise = this.page
      .waitForResponse((resp: Response) => resp.url().includes("/api/v2/dags") && resp.status() === 200, {
        timeout: 60_000,
      })
      .catch(() => {
        /* API might fail or timeout */
      });

    await this.navigateTo(DagsPage.dagsListUrl);

    // Wait for initial API response
    await responsePromise;
    await this.waitForDagList();
  }

  /**
   * Navigate to Dag detail page
   */
  public async navigateToDagDetail(dagName: string): Promise<void> {
    await this.navigateTo(DagsPage.getDagDetailUrl(dagName));
  }

  public async navigateToDagTasks(dagId: string): Promise<void> {
    await expect(async () => {
      await this.safeGoto(`/dags/${dagId}/tasks`);
      await expect(
        this.page
          .locator("th")
          .filter({ hasText: /^Operator$/ })
          .first(),
      ).toBeVisible({ timeout: 30_000 });
    }).toPass({ intervals: [2000], timeout: 60_000 });
  }

  /**
   * Search for a Dag by name
   */
  public async searchDag(searchTerm: string): Promise<void> {
    const currentNames = await this.getDagNames();

    const responsePromise = this.page
      .waitForResponse((resp: Response) => resp.url().includes("/dags") && resp.status() === 200, {
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
          const noDagFound = this.page.getByText(/no dag/i);
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
    await this.verifyTableViewVisible();
  }

  /**
   * Trigger a Dag run
   */
  public async triggerDag(dagName: string): Promise<string | null> {
    await expect(async () => {
      await this.navigateToDagDetail(dagName);
      await expect(this.triggerButton).toBeVisible({ timeout: 5000 });
    }).toPass({ intervals: [2000], timeout: 60_000 });
    await this.triggerButton.click();

    return this.handleTriggerDialog();
  }

  /**
   * Verify card view is visible
   */
  public async verifyCardViewVisible(): Promise<void> {
    await expect(this.page.locator('[data-testid="card-list"]')).toBeVisible({ timeout: 10_000 });
  }

  /**
   * Navigate to details tab and verify Dag details are displayed correctly
   */
  public async verifyDagDetails(dagName: string): Promise<void> {
    await expect(async () => {
      await this.safeGoto(`/dags/${dagName}/details`, { waitUntil: "domcontentloaded" });
      await expect(this.page.getByRole("heading", { name: dagName })).toBeVisible({ timeout: 30_000 });
    }).toPass({ intervals: [2000], timeout: 60_000 });
  }

  /**
   * Verify if a specific Dag exists in the list
   */
  public async verifyDagExists(dagId: string): Promise<void> {
    await this.waitForDagList();

    // Check which view is active
    const cardList = this.page.locator('[data-testid="card-list"]');
    const isCardView = await cardList.isVisible();

    const dagLink = isCardView
      ? this.page.locator('[data-testid="dag-id"]').filter({ hasText: dagId })
      : this.page.locator('[data-testid="table-list"] tbody tr td a').filter({ hasText: dagId });

    await expect(dagLink).toBeVisible({ timeout: 10_000 });
  }

  public async verifyDagRunStatus(dagName: string, dagRunId: string | null): Promise<void> {
    if (dagRunId === null || dagRunId === "") {
      return;
    }

    await this.safeGoto(DagsPage.getDagRunDetailsUrl(dagName, dagRunId), {
      timeout: 15_000,
      waitUntil: "domcontentloaded",
    });

    await expect
      .poll(
        async () => {
          await expect(this.stateElement).toBeVisible({ timeout: 30_000 });

          const status = await this.getCurrentDagRunStatus();

          if (status === "failed") {
            throw new Error(`Dag run failed: ${dagRunId}`);
          }

          if (status !== "success") {
            await this.page.reload({ waitUntil: "domcontentloaded" });
          }

          return status;
        },
        {
          intervals: [10_000],
          message: `Dag run did not complete within the allowed time: ${dagRunId}`,
          timeout: 7 * 60 * 1000,
        },
      )
      .toBe("success");
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
  public async verifyTableViewVisible(): Promise<void> {
    await expect(this.page.locator("table")).toBeVisible({ timeout: 10_000 });
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
    const responsePromise = this.page
      .waitForResponse(
        (response: Response) => {
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
    await expect(this.confirmButton).toBeEnabled({ timeout: 10_000 });
    await this.confirmButton.click();

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

    const option = this.page.locator(`div[role="option"][data-value="${value}"]`);

    await expect(option).toBeVisible({ timeout: 5000 });
    await option.click();

    // Ensure the dropdown closes after selection. Chakra Select may not auto-close
    // reliably across browsers, so press Escape as a fallback.
    const listbox = this.page.locator('div[role="listbox"]');

    await expect(listbox)
      .toBeHidden({ timeout: 5000 })
      .catch(async () => {
        await this.page.keyboard.press("Escape");
        await expect(filter).toHaveAttribute("data-state", "closed", { timeout: 5000 });
      });
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
    const noDagFound = this.page.getByText(/no dag/i);
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
      await expect(this.page.locator('[data-testid="dag-id"]').first()).toBeVisible({ timeout: 30_000 });
    } else {
      // Table view: prefer table-list testid, fallback to any <table> element
      const rowsInTableList = tableList.locator("tbody tr");

      await ((await rowsInTableList.count().catch(() => 0)) > 0
        ? expect(rowsInTableList.first()).toBeVisible({ timeout: 30_000 })
        : expect(fallbackTable.locator("tbody tr").first()).toBeVisible({ timeout: 30_000 }));
    }
  }
}
