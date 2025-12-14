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
import type { Locator, Page } from "@playwright/test";
import { BasePage } from "tests/e2e/pages/BasePage";

import type { DAGRunResponse } from "openapi/requests/types.gen";

/**
 * DAGs Page Object
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
   * Navigate to DAGs list page
   */
  public async navigate(): Promise<void> {
    await this.navigateTo(DagsPage.dagsListUrl);
  }

  /**
   * Navigate to DAG detail page
   */
  public async navigateToDagDetail(dagName: string): Promise<void> {
    await this.navigateTo(DagsPage.getDagDetailUrl(dagName));
  }

  /**
   * Trigger a DAG run
   */
  public async triggerDag(dagName: string): Promise<string | null> {
    await this.navigateToDagDetail(dagName);
    await this.triggerButton.waitFor({ state: "visible", timeout: 10_000 });
    await this.triggerButton.click();
    const dagRunId = await this.handleTriggerDialog();

    return dagRunId;
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
        throw new Error(`DAG run failed: ${dagRunId}`);
      }

      await this.page.waitForTimeout(checkInterval);

      await this.page.reload({ waitUntil: "domcontentloaded" });

      await this.page.waitForTimeout(2000);
    }

    throw new Error(`DAG run did not complete within 5 minutes: ${dagRunId}`);
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
