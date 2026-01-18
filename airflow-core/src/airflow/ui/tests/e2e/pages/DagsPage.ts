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

  public readonly confirmButton: Locator;
  public readonly operatorFilter: Locator;
  public readonly paginationNextButton: Locator;
  public readonly paginationPrevButton: Locator;
  public readonly retriesFilter: Locator;
  public readonly searchBox: Locator;
  public readonly stateElement: Locator;
  public readonly triggerButton: Locator;
  public readonly triggerRuleFilter: Locator;

  public get taskCards(): Locator {
    // CardList component renders a SimpleGrid with data-testid="card-list"
    // Individual cards are direct children (Box elements)
    return this.page.locator('[data-testid="card-list"] > div');
  }

  public constructor(page: Page) {
    super(page);
    this.triggerButton = page.locator('button[aria-label="Trigger Dag"]:has-text("Trigger")');
    this.confirmButton = page.locator('button:has-text("Trigger")').nth(1);
    this.stateElement = page.locator('*:has-text("State") + *').first();
    this.paginationNextButton = page.locator('[data-testid="next"]');
    this.paginationPrevButton = page.locator('[data-testid="prev"]');
    this.searchBox = page.getByRole("textbox", { name: /search/i });
    this.operatorFilter = page.getByRole("combobox").filter({ hasText: /operator/i });
    this.triggerRuleFilter = page.getByRole("combobox").filter({ hasText: /trigger/i });
    this.retriesFilter = page.getByRole("combobox").filter({ hasText: /retr/i });
  }

  // URL builders for dynamic paths
  public static getDagDetailUrl(dagName: string): string {
    return `/dags/${dagName}`;
  }

  public static getDagRunDetailsUrl(dagName: string, dagRunId: string): string {
    return `/dags/${dagName}/runs/${dagRunId}/details`;
  }

  /**
   * Click next page button
   */
  public async clickNextPage(): Promise<void> {
    const initialDagNames = await this.getDagNames();

    await this.paginationNextButton.click();

    await expect.poll(() => this.getDagNames(), { timeout: 10_000 }).not.toEqual(initialDagNames);

    await this.waitForDagList();
  }

  /**
   * Click previous page button
   */
  public async clickPrevPage(): Promise<void> {
    const initialDagNames = await this.getDagNames();

    await this.paginationPrevButton.click();

    await expect.poll(() => this.getDagNames(), { timeout: 10_000 }).not.toEqual(initialDagNames);
    await this.waitForDagList();
  }

  public async filterByOperator(operator: string): Promise<void> {
    await this.selectDropdownOption(this.operatorFilter, operator);
  }

  public async filterByRetries(retries: string): Promise<void> {
    await this.selectDropdownOption(this.retriesFilter, retries);
  }

  public async filterByTriggerRule(rule: string): Promise<void> {
    await this.selectDropdownOption(this.triggerRuleFilter, rule);
  }

  /**
   * Get all Dag names from the current page
   */
  public async getDagNames(): Promise<Array<string>> {
    await this.waitForDagList();
    const dagLinks = this.page.locator('[data-testid="dag-id"]');
    const texts = await dagLinks.allTextContents();

    return texts.map((text) => text.trim()).filter((text) => text !== "");
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
    await this.navigateTo(DagsPage.dagsListUrl);
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
      .locator("h2")
      .filter({ hasText: /^Operator$/ })
      .first()
      .waitFor({ state: "visible", timeout: 30_000 });
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
    await expect(this.page.locator('[data-testid="dag-id"]').first()).toBeVisible({
      timeout: 10_000,
    });
  }
}
