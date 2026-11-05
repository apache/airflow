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

import type { ClearTaskInstancesBody } from "openapi/requests/types.gen";

import { BasePage } from "./BasePage";

const FORCE_RUN_LABEL = "Force run (ignore upstream dependencies)";

export class TaskInstancePage extends BasePage {
  public readonly clearTaskInstanceButton: Locator;
  public readonly confirmClearButton: Locator;
  public readonly confirmTriggerButton: Locator;
  public readonly downstreamOption: Locator;
  public readonly forceRunCheckbox: Locator;
  public readonly forceRunCheckboxLabel: Locator;
  public readonly forceRunWarning: Locator;
  public readonly onlyFailedOption: Locator;
  public readonly stateBadge: Locator;
  public readonly triggerButton: Locator;
  public readonly upstreamOption: Locator;

  public constructor(page: Page) {
    super(page);
    this.triggerButton = page.getByTestId("trigger-dag-button");
    this.confirmTriggerButton = page.getByTestId("trigger-dag-submit");
    this.stateBadge = page.getByTestId("header-card").getByTestId("state-badge").first();
    this.clearTaskInstanceButton = page.getByTestId("clear-task-instance-button");
    this.forceRunCheckbox = page.getByRole("checkbox", { name: FORCE_RUN_LABEL });
    // Chakra's Checkbox.Root is the <label for="<input id>"> wrapping the control and the text.
    this.forceRunCheckboxLabel = page
      .locator('label[data-scope="checkbox"][data-part="root"]')
      .filter({ hasText: FORCE_RUN_LABEL });
    this.forceRunWarning = page.getByText("Runs this task even though its upstream tasks did not succeed", {
      exact: false,
    });
    this.upstreamOption = page.getByRole("button", { exact: true, name: "Upstream" });
    this.downstreamOption = page.getByRole("button", { exact: true, name: "Downstream" });
    this.onlyFailedOption = page.getByRole("button", { exact: true, name: "Clear only failed tasks" });
    this.confirmClearButton = page.getByRole("button", { name: "Confirm" });
  }

  /**
   * Opens the clear dialog, ticks "Force run", confirms the upstream-bypass
   * warning and the disabled upstream/downstream options, then submits.
   */
  public async forceRun(): Promise<void> {
    await this.clearTaskInstanceButton.click();
    await expect(this.forceRunCheckboxLabel).toBeVisible();
    // Click the label, not the input: Chakra renders the input visually hidden at 1x1 px, and
    // Playwright discards a click target whose viewport-clipped area is not > 0.99 px². The
    // dialog's scale-in animation shrinks it to 0.95 px, which Firefox measures and Chromium
    // does not. Leave the click unforced so it also waits for that animation to settle.
    await this.forceRunCheckboxLabel.click();
    await expect(this.forceRunCheckbox).toBeChecked();
    await expect(this.forceRunWarning).toBeVisible();
    await expect(this.upstreamOption).toBeDisabled();
    await expect(this.downstreamOption).toBeDisabled();
    await expect(this.onlyFailedOption).toBeDisabled();

    // Confirming re-opens the dialog for a dry-run preview, which also POSTs to
    // /clearTaskInstances (dry_run: true) before the real clear fires — exclude it so we don't
    // resolve on that earlier response instead of the actual clear.
    const clearResponse = this.page.waitForResponse(
      (response) =>
        response.url().includes("/clearTaskInstances") &&
        response.request().method() === "POST" &&
        (response.request().postDataJSON() as ClearTaskInstancesBody | null)?.dry_run !== true,
    );

    await this.confirmClearButton.click();
    await clearResponse;
    await this.waitForAllDialogsClosed();
  }

  public async navigateToDag(dagId: string): Promise<void> {
    await expect(async () => {
      await this.navigateTo(`/dags/${dagId}`);
      await expect(this.triggerButton).toBeVisible({ timeout: 5000 });
    }).toPass({ intervals: [2000], timeout: 60_000 });
  }

  public async navigateToTaskInstance(dagId: string, runId: string, taskId: string): Promise<void> {
    await expect(async () => {
      await this.navigateTo(`/dags/${dagId}/runs/${runId}/tasks/${taskId}`);
      // #details-panel content depends on chained API calls that can be slow on WebKit.
      await expect(this.page.locator("#details-panel")).toBeVisible();
    }).toPass({ intervals: [2000], timeout: 60_000 });
  }

  public async navigateToTaskInstanceDetails(dagId: string, runId: string, taskId: string): Promise<void> {
    await expect(async () => {
      await this.navigateTo(`/dags/${dagId}/runs/${runId}/tasks/${taskId}/details`);
      await expect(this.page.locator("#details-panel")).toBeVisible();
    }).toPass({ intervals: [2000], timeout: 60_000 });
  }

  public async triggerDagAndWaitForSuccess(dagId: string): Promise<void> {
    await this.triggerDagRun(dagId);
    await this.waitForDagRunSuccess();
  }

  public async triggerDagRun(dagId: string): Promise<void> {
    await this.navigateToDag(dagId);
    await this.triggerButton.click();
    await this.confirmTriggerButton.click();
    await this.page.waitForURL(/.*\/runs\/.*/, { timeout: 15_000 });
  }

  public async waitForDagRunSuccess(): Promise<void> {
    await expect(this.stateBadge).toContainText("Success", { timeout: 60_000 });
  }
}
