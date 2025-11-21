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

import { BasePage } from "./BasePage";

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
  public readonly stateElement: Locator;
  public readonly triggerButton: Locator;

  public constructor(page: Page) {
    // eslint-disable-next-line @typescript-eslint/no-unsafe-call
    super(page);
    this.dagsTable = page.locator('div:has(a[href*="/dags/"])');
    this.triggerButton = page.locator('button[aria-label="Trigger Dag"]:has-text("Trigger")');
    this.confirmButton = page.locator('button:has-text("Trigger")').nth(1);
    this.stateElement = page.locator('*:has-text("State") + *').first();
  }

  // URL builders for dynamic paths
  public static getDagDetailUrl(dagName: string): string {
    return `/dags/${dagName}`;
  }

  public static getDagRunDetailsUrl(dagName: string, dagRunId: string): string {
    return `/dags/${dagName}/runs/${dagRunId}/details`;
  }

  /**
   * Navigate to DAGs list page
   */
  public async navigate(): Promise<void> {
    // eslint-disable-next-line @typescript-eslint/no-unsafe-call
    await this.navigateTo(DagsPage.dagsListUrl);
  }

  /**
   * Navigate to DAG detail page
   */
  public async navigateToDagDetail(dagName: string): Promise<void> {
    // eslint-disable-next-line @typescript-eslint/no-unsafe-call
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

    // eslint-disable-next-line @typescript-eslint/no-unsafe-call, @typescript-eslint/no-unsafe-member-access
    await this.page.goto(DagsPage.getDagRunDetailsUrl(dagName, dagRunId), {
      timeout: 15_000,
      waitUntil: "domcontentloaded",
    });
    // eslint-disable-next-line @typescript-eslint/no-unsafe-call, @typescript-eslint/no-unsafe-member-access
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

      // eslint-disable-next-line @typescript-eslint/no-unsafe-call, @typescript-eslint/no-unsafe-member-access
      await this.page.waitForTimeout(checkInterval);
      // eslint-disable-next-line @typescript-eslint/no-unsafe-call, @typescript-eslint/no-unsafe-member-access
      await this.page.reload({ waitUntil: "domcontentloaded" });
      // eslint-disable-next-line @typescript-eslint/no-unsafe-call, @typescript-eslint/no-unsafe-member-access
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
    // eslint-disable-next-line @typescript-eslint/no-unsafe-call, @typescript-eslint/no-unsafe-member-access
    await this.page.waitForTimeout(1000);

    // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment, @typescript-eslint/no-unsafe-call
    const responsePromise = this.page
      // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access
      .waitForResponse(
        (response) => {
          // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment, @typescript-eslint/no-unsafe-call, @typescript-eslint/no-unsafe-member-access
          const url = response.url();
          // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment, @typescript-eslint/no-unsafe-call, @typescript-eslint/no-unsafe-member-access
          const method = response.request().method();

          return (
            // eslint-disable-next-line @typescript-eslint/no-unsafe-call, @typescript-eslint/no-unsafe-member-access, @typescript-eslint/strict-boolean-expressions
            method === "POST" && Boolean(url.includes("dagRuns")) && Boolean(!url.includes("hitlDetails"))
          );
        },
        { timeout: 10_000 },
      )
      // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access
      .catch(() => undefined);

    await this.confirmButton.waitFor({ state: "visible", timeout: 8000 });
    // eslint-disable-next-line @typescript-eslint/no-unsafe-call, @typescript-eslint/no-unsafe-member-access
    await this.page.waitForTimeout(2000);
    await this.confirmButton.click({ force: true });

    // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment
    const apiResponse = await responsePromise;

    // eslint-disable-next-line @typescript-eslint/strict-boolean-expressions
    if (apiResponse) {
      try {
        // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment, @typescript-eslint/no-unsafe-call, @typescript-eslint/no-unsafe-member-access
        const responseBody = await apiResponse.text();
        // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment, @typescript-eslint/no-unsafe-argument
        const responseJson = JSON.parse(responseBody);

        // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access
        if (Boolean(responseJson.dag_run_id)) {
          // eslint-disable-next-line @typescript-eslint/no-unsafe-return, @typescript-eslint/no-unsafe-member-access
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
