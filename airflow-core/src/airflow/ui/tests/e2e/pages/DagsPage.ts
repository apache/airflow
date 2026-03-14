/*!
 * Licensed to the Apache Software Foundation (ASF)
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

export class DagsPage extends BasePage {

  public static get dagsListUrl(): string {
    return "/dags";
  }

  public readonly triggerButton: Locator;
  public readonly confirmButton: Locator;
  public readonly stateElement: Locator;

  public readonly operatorFilter: Locator;
  public readonly triggerRuleFilter: Locator;
  public readonly retriesFilter: Locator;

  public readonly searchBox: Locator;
  public readonly searchInput: Locator;

  public readonly cardViewButton: Locator;
  public readonly tableViewButton: Locator;

  public readonly successFilter: Locator;
  public readonly failedFilter: Locator;
  public readonly runningFilter: Locator;
  public readonly queuedFilter: Locator;
  public readonly needsReviewFilter: Locator;

  public get taskRows(): Locator {
    return this.page.locator('[data-testid="table-list"] > tbody > tr');
  }

  public constructor(page: Page) {
    super(page);

    this.triggerButton = page.locator('button[aria-label="Trigger Dag"]:has-text("Trigger")');
    this.confirmButton = page.locator('button:has-text("Trigger")').last();

    this.stateElement = page.locator('*:has-text("State") + *').first();

    this.searchBox = page.getByRole("textbox", { name: /search/i });
    this.searchInput = page.getByPlaceholder("Search DAGs");

    this.operatorFilter = page.getByRole("combobox").filter({ hasText: /operator/i });
    this.triggerRuleFilter = page.getByRole("combobox").filter({ hasText: /trigger/i });
    this.retriesFilter = page.getByRole("combobox").filter({ hasText: /retr/i });

    this.cardViewButton = page.locator('button[aria-label="Show card view"]');
    this.tableViewButton = page.locator('button[aria-label="Show table view"]');

    this.successFilter = page.locator('button:has-text("Success")');
    this.failedFilter = page.locator('button:has-text("Failed")');
    this.runningFilter = page.locator('button:has-text("Running")');
    this.queuedFilter = page.locator('button:has-text("Queued")');
    this.needsReviewFilter = page.locator('button:has-text("Needs Review")');
  }

  public static getDagDetailUrl(dagName: string): string {
    return `/dags/${dagName}`;
  }

  public static getDagRunDetailsUrl(dagName: string, dagRunId: string): string {
    return `/dags/${dagName}/runs/${dagRunId}/details`;
  }

  public async navigateToDagDetail(dagName: string): Promise<void> {
    await this.navigateTo(DagsPage.getDagDetailUrl(dagName));
  }

  public async navigateToDagTasks(dagId: string): Promise<void> {
    await this.page.goto(`/dags/${dagId}/tasks`);
    await this.page.locator("th").filter({ hasText: /^Operator$/ }).first().waitFor({
      state: "visible",
      timeout: 30000,
    });
  }

  public async filterByOperator(operator: string): Promise<void> {
    await this.selectDropdownOption(this.operatorFilter, operator);
  }

  public async filterByTriggerRule(rule: string): Promise<void> {
    await this.selectDropdownOption(this.triggerRuleFilter, rule);
  }

  public async filterByRetries(retries: string): Promise<void> {
    await this.selectDropdownOption(this.retriesFilter, retries);
  }

  public async getFilterOptions(filter: Locator): Promise<Array<string>> {
    await filter.click();

    const options = this.page.locator('div[role="option"]');
    const count = await options.count();

    const values: string[] = [];

    for (let i = 0; i < count; i++) {
      const value = await options.nth(i).getAttribute("data-value");
      if (value) {
        values.push(value);
      }
    }

    await this.page.keyboard.press("Escape");

    return values;
  }

  public async triggerDag(dagName: string): Promise<string | null> {
    await this.navigateToDagDetail(dagName);

    await expect(this.triggerButton).toBeVisible();
    await this.triggerButton.click();

    return await this.handleTriggerDialog();
  }

  public async verifyDagRunStatus(dagName: string, dagRunId: string | null): Promise<void> {
    if (!dagRunId) {
      return;
    }

    await this.page.goto(DagsPage.getDagRunDetailsUrl(dagName, dagRunId), {
      waitUntil: "domcontentloaded",
    });

    await this.page.waitForLoadState("networkidle");

    const maxWaitTime = 7 * 60 * 1000;
    const startTime = Date.now();

    while (Date.now() - startTime < maxWaitTime) {

      const status = await this.getCurrentDagRunStatus();

      if (status === "success") {
        return;
      }

      if (status === "failed") {
        throw new Error(`Dag run failed: ${dagRunId}`);
      }

      await this.page.reload({ waitUntil: "domcontentloaded" });
      await this.page.waitForLoadState("networkidle");
    }

    throw new Error(`Dag run did not complete within 5 minutes: ${dagRunId}`);
  }

  private async getCurrentDagRunStatus(): Promise<string> {
    const statusText = await this.stateElement.textContent();
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
  }

  private async handleTriggerDialog(): Promise<string | null> {

  const responsePromise = this.page.waitForResponse((response: any) => {
    const url = response.url();
    const method = response.request().method();

    return method === "POST" && url.includes("dagRuns");
  });

  await expect(this.confirmButton).toBeVisible();
  await this.confirmButton.click({ force: true });

  const response = await responsePromise;

  try {
    const json = (await response.json()) as DAGRunResponse;

    if (json.dag_run_id) {
      return json.dag_run_id;
    }
  } catch {
    return null;
  }

  return null;
}
  private async selectDropdownOption(filter: Locator, value: string): Promise<void> {
    await filter.click();
    await this.page.locator(`div[role="option"][data-value="${value}"]`).click();
    await this.page.waitForLoadState("networkidle");
  }
}