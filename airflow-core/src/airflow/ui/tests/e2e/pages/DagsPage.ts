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

  // Core page elements
  public readonly confirmButton: Locator;
  public readonly dagsTable: Locator;
  // Pagination elements
  public readonly paginationNextButton: Locator;
  public readonly paginationPrevButton: Locator;
  // Runs tab elements
  public readonly runsTab: Locator;
  public readonly runsTable: Locator;

  public readonly stateElement: Locator;
  public readonly triggerButton: Locator;

  public constructor(page: Page) {
    super(page);
    this.dagsTable = page.locator('div:has(a[href*="/dags/"])');
    this.triggerButton = page.locator('button[aria-label="Trigger Dag"]:has-text("Trigger")');
    this.confirmButton = page.locator('button:has-text("Trigger")').nth(1);
    this.stateElement = page.locator('*:has-text("State") + *').first();
    this.paginationNextButton = page.locator('[data-testid="next"]');
    this.paginationPrevButton = page.locator('[data-testid="prev"]');
    this.runsTab = page.locator('a[href$="/runs"]');
    this.runsTable = page.locator('[data-testid="dag-runs-table"]');
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
    await this.paginationNextButton.click();
    await this.waitForPageLoad();
  }

  /**
   * Click previous page button
   */
  public async clickPrevPage(): Promise<void> {
    await this.paginationPrevButton.click();
    await this.waitForPageLoad();
  }

  /**
   * Get all Dag names from the current page
   */
  public async getDagNames(): Promise<Array<string>> {
    const dagLinks = this.page.locator('[data-testid="dag-id"]');

    await dagLinks.first().waitFor({ state: "visible", timeout: 10_000 });
    const texts = await dagLinks.allTextContents();

    return texts.map((text) => text.trim()).filter((text) => text !== "");
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
   * Trigger a Dag run
   */
  public async triggerDag(dagName: string): Promise<string | null> {
    await this.navigateToDagDetail(dagName);
    await this.triggerButton.waitFor({ state: "visible", timeout: 10_000 });
    await this.triggerButton.click();
    const dagRunId = await this.handleTriggerDialog();

    return dagRunId;
  }

  /**
   * Navigate to details tab and verify Dag details are displayed correctly
   */
  /**
   * Navigate to the Runs tab for a specific DAG
   */
  public async navigateToRunsTab(dagName: string): Promise<void> {
    await this.navigateToDagDetail(dagName);
    await this.runsTab.waitFor({ state: "visible" });
    await this.runsTab.click();
    await this.waitForPageLoad();
  }

  /**
   * Verify the Runs tab is displayed correctly
   */
  public async verifyRunsTabDisplayed(): Promise<void> {
    // Verify the runs table is present
    await expect(this.runsTable).toBeVisible();

    // Verify pagination controls are visible
    await expect(this.paginationNextButton).toBeVisible();
    await expect(this.paginationPrevButton).toBeVisible();
  }

  /**
   * Get run details from the runs table
   */
  public async getRunDetails(): Promise<
    Array<{
      runId: string;
      state: string;
    }>
  > {
    const runRows = this.page.locator('[data-testid="dag-runs-table"] table tbody tr');
    await runRows.first().waitFor({ state: "visible", timeout: 10_000 });

    const runCount = await runRows.count();
    const runs: Array<{ runId: string; state: string }> = [];

    for (let i = 0; i < runCount; i++) {
      const row = runRows.nth(i);
      const cells = row.locator("td");

      // Assuming first column is run ID and state column exists
      const runId = (await cells.nth(0).textContent()) ?? "";
      const state = (await cells.nth(1).textContent()) ?? "";

      runs.push({ runId: runId.trim(), state: state.trim() });
    }

    return runs;
  }

  /**
   * Click on a specific run to view details
   */
  public async clickRun(runId: string): Promise<void> {
    const runLink = this.page.locator(`a:has-text("${runId}")`).first();

    await runLink.waitFor({ state: "visible" });
    await runLink.click();
    await this.waitForPageLoad();
  }

  /**
   * Filter runs by state
   */
  public async filterByState(state: string): Promise<void> {
    // Click on the state filter
    const stateFilter = this.page.locator('button:has-text("State")');

    await stateFilter.waitFor({ state: "visible" });
    await stateFilter.click();

    // Select the specific state
    const stateOption = this.page.locator(`[role="option"]:has-text("${state}")`);

    await stateOption.waitFor({ state: "visible" });
    await stateOption.click();

    await this.waitForPageLoad();
  }

  /**
   * Search for dag runs by run ID pattern
   */
  public async searchRun(searchTerm: string): Promise<void> {
    // Find the run ID pattern input field
    const searchInput = this.page.locator('input[placeholder*="Run ID"]').or(
      this.page.locator('input[name="runIdPattern"]'),
    );

    await searchInput.waitFor({ state: "visible" });
    await searchInput.fill(searchTerm);

    // Wait for the search to take effect
    await this.page.waitForTimeout(1000);
    await this.waitForPageLoad();
  }

  /**
   * Verify we're on the run details page
   */
  public async verifyRunDetailsPage(runId: string): Promise<void> {
    // Wait for the page to load
    await this.waitForPageLoad();

    // Verify URL contains the run ID
    await expect(this.page).toHaveURL(new RegExp(`/runs/${runId}`));
  }

  public async verifyDagDetails(dagName: string): Promise<void> {
    await this.navigateToDagDetail(dagName);

    const detailsTab = this.page.locator('a[href$="/details"]');

    await detailsTab.waitFor({ state: "visible" });
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

    await this.confirmButton.waitFor({ state: "visible", timeout: 8000 });

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
}
