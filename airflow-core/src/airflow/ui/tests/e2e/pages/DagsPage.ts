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

    this.searchBox = page.getByRole("textbox", { name: /search/i });
    this.searchInput = page.getByPlaceholder("Search Dags");
    this.operatorFilter = page.getByTestId("operator-filter");
    this.triggerRuleFilter = page.getByTestId("trigger-rule-filter");
    this.retriesFilter = page.getByTestId("retries-filter");
    this.cardViewButton = page.getByRole("button", { name: "Show card view" });
    this.tableViewButton = page.getByRole("button", { name: "Show table view" });
    this.successFilter = page.getByRole("button", { name: "Success" });
    this.failedFilter = page.getByRole("button", { name: "Failed" });
    this.runningFilter = page.getByRole("button", { name: "Running" });
    this.queuedFilter = page.getByRole("button", { name: "Queued" });
    // Uses testId because this button's text is driven by an i18n key.
    this.needsReviewFilter = page.getByTestId("dags-needs-review-filter");
  }

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
      .catch((error: unknown) => {
        if (error instanceof Error && !error.message.includes("Timeout")) {
          throw error;
        }
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
   * Filter Dags by status
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
      .catch((error: unknown) => {
        if (error instanceof Error && !error.message.includes("Timeout")) {
          throw error;
        }
      });

    await filterMap[status].click();
    await responsePromise;
  }

  public async filterByTriggerRule(rule: string): Promise<void> {
    await this.selectDropdownOption(this.triggerRuleFilter, rule);
  }

  /**
   * Get a locator for a specific Dag link in the list (card or table view).
   */
  public getDagLink(dagId: string): Locator {
    // Card view locator OR table view locator — only one will be visible.
    return this.page
      .locator('[data-testid="dag-id"]')
      .filter({ hasText: dagId })
      .or(this.page.locator('[data-testid="table-list"] tbody tr td a').filter({ hasText: dagId }));
  }

  /**
   * Get all Dag links from the list
   */
  public async getDagLinks(): Promise<Array<string>> {
    await this.waitForDagList();

    const cardList = this.page.locator('[data-testid="card-list"]');
    const isCardView = await cardList.isVisible();

    const links = isCardView
      ? await this.page.locator('[data-testid="dag-id"]').all()
      : await this.page.getByTestId("table-list").locator('tbody td a[href*="/dags/"]').all();

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

    const cardList = this.page.locator('[data-testid="card-list"]');
    const isCardView = await cardList.isVisible();

    const dagLinks = isCardView
      ? this.page.locator('[data-testid="dag-id"]')
      : this.page.getByTestId("table-list").locator('tbody td a[href*="/dags/"]');

    const texts = await dagLinks.allTextContents();

    return texts.map((text) => text.trim()).filter((text) => text !== "");
  }

  /**
   * Get count of Dags on current page
   */
  public async getDagsCount(): Promise<number> {
    await this.waitForDagList();

    const cardList = this.page.locator('[data-testid="card-list"]');
    const isCardView = await cardList.isVisible();

    if (isCardView) {
      return this.page.locator('[data-testid="dag-id"]').count();
    }

    return this.page.getByTestId("table-list").locator("tbody tr").count();
  }

  public async getFilterOptions(filter: Locator): Promise<Array<string>> {
    const state = await filter.getAttribute("data-state");

    if (state === "open") {
      await this.page.keyboard.press("Escape");
      await expect(filter).toHaveAttribute("data-state", "closed", { timeout: 5000 });
    }

    await expect(async () => {
      await filter.click({ timeout: 5000 });
      await expect(filter).toHaveAttribute("data-state", "open", { timeout: 5000 });
    }).toPass({ intervals: [1000, 2000], timeout: 15_000 });

    const controlsId = await filter.getAttribute("aria-controls");

    const dropdown =
      controlsId === null
        ? this.page.locator('div[role="listbox"][data-state="open"]').first()
        : this.page.locator(`[id="${controlsId}"]`);
    const options = dropdown.locator('div[role="option"]');

    await expect(options.first()).toBeVisible({ timeout: 10_000 });

    const count = await options.count();
    const dataValues: Array<string> = [];

    for (let i = 0; i < count; i++) {
      const value = await options.nth(i).getAttribute("data-value");

      if (value !== null && value.trim().length > 0) {
        dataValues.push(value);
      }
    }

    // Click outside to dismiss — Escape is unreliable on WebKit.
    await this.page.locator("body").click({ position: { x: 0, y: 0 } });

    await expect(dropdown.and(this.page.locator('[data-state="open"]'))).toBeHidden({ timeout: 5000 });

    return dataValues;
  }

  /**
   * Navigate to Dags list page
   */
  public async navigate(): Promise<void> {
    await expect(async () => {
      await this.navigateTo(DagsPage.dagsListUrl);
      await this.waitForDagList();
    }).toPass({ intervals: [2000], timeout: 60_000 });
  }

  /**
   * Navigate to Dag detail page
   */
  public async navigateToDagDetail(dagName: string): Promise<void> {
    await expect(async () => {
      await this.page.goto(DagsPage.getDagDetailUrl(dagName), { waitUntil: "domcontentloaded" });

      const hydrationSignal = this.page
        .getByRole("heading", { name: dagName })
        .or(this.page.locator(`[data-testid="dag-name"]:has-text("${dagName}")`))
        .first();

      await expect(hydrationSignal).toBeVisible({ timeout: 10_000 });
    }).toPass({ intervals: [2000], timeout: 60_000 });
  }

  /**
   * Navigate to details tab and wait for heading to appear
   */
  public async navigateToDagDetails(dagName: string): Promise<void> {
    await expect(async () => {
      await this.page.goto(`/dags/${dagName}/details`, { waitUntil: "domcontentloaded" });
      await expect(this.page.getByRole("heading", { name: dagName })).toBeVisible({ timeout: 30_000 });
    }).toPass({ intervals: [2000], timeout: 60_000 });
  }

  public async navigateToDagTasks(dagId: string): Promise<void> {
    await expect(async () => {
      await this.page.goto(`/dags/${dagId}/tasks`, { waitUntil: "domcontentloaded" });
      await expect(this.page.getByRole("columnheader", { name: "Operator" })).toBeVisible({
        timeout: 30_000,
      });
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
      .catch((error: unknown) => {
        if (error instanceof Error && !error.message.includes("Timeout")) {
          throw error;
        }
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
    await expect(this.cardViewButton).toBeVisible({ timeout: 30_000 });
    await expect(this.cardViewButton).toBeEnabled({ timeout: 10_000 });
    await this.cardViewButton.click();
    await this.waitForCardView();
  }

  /**
   * Switch to table view
   */
  public async switchToTableView(): Promise<void> {
    await expect(this.tableViewButton).toBeVisible({ timeout: 30_000 });
    await expect(this.tableViewButton).toBeEnabled({ timeout: 10_000 });
    await this.tableViewButton.click();
    await this.waitForTableView();
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
   * Wait for card view to be visible
   */
  public async waitForCardView(): Promise<void> {
    await expect(this.page.locator('[data-testid="card-list"]')).toBeVisible({ timeout: 10_000 });
  }

  /**
   * Wait for Dag list to be rendered (card view, table view, or empty state).
   */
  public async waitForDagList(): Promise<void> {
    // Wait for actual rendered content — not skeletons, not bare table rows.
    // Card view: dag-id links only render after data loads.
    const dagCard = this.page.locator('[data-testid="dag-id"]').first();
    const dagLink = this.page.locator('tbody td a[href*="/dags/"]').first();
    const noDagFound = this.page.getByText(/no dag/i);

    await expect(dagCard.or(dagLink).or(noDagFound)).toBeVisible({ timeout: 60_000 });
  }

  /**
   * Wait for table view to be visible
   */
  public async waitForTableView(): Promise<void> {
    await expect(this.page.locator("table")).toBeVisible({ timeout: 10_000 });
  }

  private async handleTriggerDialog(): Promise<string | null> {
    await expect(this.confirmButton).toBeVisible({ timeout: 8000 });
    await expect(this.confirmButton).toBeEnabled({ timeout: 10_000 });

    const responsePromise = this.page.waitForResponse(
      (response: Response) => {
        const url = response.url();
        const method = response.request().method();

        return method === "POST" && Boolean(url.includes("dagRuns")) && Boolean(!url.includes("hitlDetails"));
      },
      { timeout: 30_000 },
    );

    await this.confirmButton.click();

    const apiResponse = await responsePromise;

    try {
      const responseBody = await apiResponse.text();
      const responseJson = JSON.parse(responseBody) as DAGRunResponse;

      if (Boolean(responseJson.dag_run_id)) {
        return responseJson.dag_run_id;
      }
    } catch {
      // Response parsing failed — return null so caller can handle.
    }

    // eslint-disable-next-line unicorn/no-null
    return null;
  }

  private async selectDropdownOption(filter: Locator, value: string): Promise<void> {
    await expect(async () => {
      // Dismiss any open dropdown/overlay before clicking the target filter.
      const currentState = await filter.getAttribute("data-state");

      if (currentState === "open") {
        await this.page.keyboard.press("Escape");
        await expect(filter).toHaveAttribute("data-state", "closed", { timeout: 5000 });
      }

      await filter.click({ timeout: 5000 });
      await expect(filter).toHaveAttribute("data-state", "open", { timeout: 5000 });

      const option = this.page.locator(`div[role="option"][data-value="${value}"]`);

      await expect(option).toBeVisible({ timeout: 5000 });
      await option.click();

      if ((await filter.getAttribute("aria-expanded")) === "true") {
        await this.page.keyboard.press("Escape");
      }

      await expect(filter).toHaveAttribute("data-state", "closed", { timeout: 5000 });
    }).toPass({ intervals: [1000, 2000], timeout: 30_000 });
  }
}
