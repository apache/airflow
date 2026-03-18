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

import { BasePage } from "./BasePage";
import { DagsPage } from "./DagsPage";

export class RequiredActionsPage extends BasePage {
  public readonly actionsTable: Locator;
  public readonly emptyStateMessage: Locator;
  public readonly pageHeading: Locator;

  public constructor(page: Page) {
    super(page);
    this.pageHeading = page.getByRole("heading").filter({ hasText: /required action/i });
    this.actionsTable = page.getByTestId("table-list");
    this.emptyStateMessage = page.getByText(/no required actions found/i);
  }

  public static getRequiredActionsUrl(): string {
    return "/required_actions";
  }

  public async getActionsTableRowCount(): Promise<number> {
    const rows = this.page.locator("table tbody tr");
    const isTableVisible = await this.actionsTable.isVisible();

    if (!isTableVisible) {
      return 0;
    }

    return rows.count();
  }

  public async isEmptyStateDisplayed(): Promise<boolean> {
    return this.emptyStateMessage.isVisible();
  }

  public async isTableDisplayed(): Promise<boolean> {
    return this.actionsTable.isVisible();
  }

  public async navigateToRequiredActionsPage(): Promise<void> {
    await this.navigateTo(RequiredActionsPage.getRequiredActionsUrl());
    await expect(this.pageHeading).toBeVisible({ timeout: 10_000 });
  }

  public async runHITLFlowWithApproval(dagId: string): Promise<void> {
    await this.runHITLFlow(dagId, true);
  }

  public async runHITLFlowWithRejection(dagId: string): Promise<void> {
    await this.runHITLFlow(dagId, false);
  }

  private async clickButtonAndWaitForHITLResponse(button: Locator): Promise<void> {
    const responsePromise = this.page.waitForResponse(
      (res) => res.url().includes("hitlDetails") && res.request().method() === "PATCH",
      { timeout: 30_000 },
    );

    await button.click();
    await responsePromise;
    await this.page.waitForTimeout(10_000);
  }

  private async clickOnTaskInGrid(dagRunId: string, taskId: string): Promise<void> {
    const taskLocator = this.page.locator(`[id="grid-${dagRunId}-${taskId}"]`);

    await expect(taskLocator).toBeVisible({ timeout: 30_000 });
    await taskLocator.click();
  }

  private async handleApprovalTask(dagId: string, dagRunId: string, approve: boolean): Promise<void> {
    await this.waitForTaskState(dagRunId, { expectedState: "Deferred", taskId: "valid_input_and_options" });

    const requiredActionLink = this.page.getByRole("link", { name: /required action/i });

    await expect(requiredActionLink).toBeVisible({ timeout: 30_000 });
    await requiredActionLink.click();

    const buttonName = approve ? "Approve" : "Reject";
    const actionButton = this.page.locator(`[data-testid="hitl-option-${buttonName}"]`);

    await expect(actionButton).toBeVisible({ timeout: 10_000 });

    const informationInput = this.page.locator("#element_information");

    if (await informationInput.isVisible()) {
      await informationInput.fill("Approved by test");
    }

    await expect(actionButton).toBeEnabled({ timeout: 10_000 });
    await this.clickButtonAndWaitForHITLResponse(actionButton);

    await this.page.goto(`/dags/${dagId}/runs/${dagRunId}`);
    await this.waitForTaskState(dagRunId, { expectedState: "Success", taskId: "valid_input_and_options" });
  }

  private async handleBranchTask(dagId: string, dagRunId: string): Promise<void> {
    await this.waitForTaskState(dagRunId, { expectedState: "Deferred", taskId: "choose_a_branch_to_run" });

    const requiredActionLink = this.page.getByRole("link", { name: /required action/i });

    await expect(requiredActionLink).toBeVisible({ timeout: 30_000 });
    await requiredActionLink.click();

    const branchButton = this.page.locator('[data-testid="hitl-option-task_1"]');

    await expect(branchButton).toBeVisible({ timeout: 10_000 });
    await this.clickButtonAndWaitForHITLResponse(branchButton);

    await this.page.goto(`/dags/${dagId}/runs/${dagRunId}`);
    await this.waitForTaskState(dagRunId, { expectedState: "Success", taskId: "choose_a_branch_to_run" });
  }

  private async handleWaitForInputTask(dagId: string, dagRunId: string): Promise<void> {
    await this.waitForTaskState(dagRunId, { expectedState: "Deferred", taskId: "wait_for_input" });

    const requiredActionLink = this.page.getByRole("link", { name: /required action/i });

    await expect(requiredActionLink).toBeVisible({ timeout: 30_000 });
    await requiredActionLink.click();

    const informationInput = this.page.locator("#element_information");

    await expect(informationInput).toBeVisible({ timeout: 10_000 });
    await informationInput.fill("test");

    const okButton = this.page.getByRole("button", { name: "OK" });

    await expect(okButton).toBeVisible({ timeout: 10_000 });
    await this.clickButtonAndWaitForHITLResponse(okButton);

    await this.page.goto(`/dags/${dagId}/runs/${dagRunId}`);
    await this.waitForTaskState(dagRunId, { expectedState: "Success", taskId: "wait_for_input" });
  }

  private async handleWaitForMultipleOptionsTask(dagId: string, dagRunId: string): Promise<void> {
    await this.waitForTaskState(dagRunId, { expectedState: "Deferred", taskId: "wait_for_multiple_options" });

    const requiredActionLink = this.page.getByRole("link", { name: /required action/i });

    await expect(requiredActionLink).toBeVisible({ timeout: 30_000 });
    await requiredActionLink.click();

    const multiSelectContainer = this.page.locator("#element_chosen_options").locator("..");

    await expect(multiSelectContainer).toBeVisible({ timeout: 30_000 });
    await multiSelectContainer.click();

    await this.page.getByRole("option", { name: "option 4" }).click();
    await multiSelectContainer.click();
    await this.page.getByRole("option", { name: "option 5" }).click();

    const respondButton = this.page.getByRole("button", { name: "Respond" });

    await expect(respondButton).toBeVisible({ timeout: 10_000 });
    await this.clickButtonAndWaitForHITLResponse(respondButton);

    await this.page.goto(`/dags/${dagId}/runs/${dagRunId}`);
    await this.waitForTaskState(dagRunId, { expectedState: "Success", taskId: "wait_for_multiple_options" });
  }

  private async handleWaitForOptionTask(dagId: string, dagRunId: string): Promise<void> {
    await this.waitForTaskState(dagRunId, { expectedState: "Deferred", taskId: "wait_for_option" });

    const requiredActionLink = this.page.getByRole("link", { name: /required action/i });

    await expect(requiredActionLink).toBeVisible({ timeout: 30_000 });
    await requiredActionLink.click();

    const optionButton = this.page.locator('[data-testid="hitl-option-option 1"]');

    await expect(optionButton).toBeVisible({ timeout: 10_000 });
    await this.clickButtonAndWaitForHITLResponse(optionButton);

    await this.page.goto(`/dags/${dagId}/runs/${dagRunId}`);
    await this.waitForTaskState(dagRunId, { expectedState: "Success", taskId: "wait_for_option" });
  }

  private async runHITLFlow(dagId: string, approve: boolean): Promise<void> {
    const dagsPage = new DagsPage(this.page);

    const dagRunId = await dagsPage.triggerDag(dagId);

    if (dagRunId === null) {
      throw new Error("Failed to trigger DAG - dagRunId is null");
    }

    await this.page.goto(`/dags/${dagId}/runs/${dagRunId}`);
    await this.waitForDagRunState("Running");

    await this.waitForTaskState(dagRunId, {
      expectedState: "Success",
      taskId: "wait_for_default_option",
      timeout: 30_000,
    });

    await this.handleWaitForInputTask(dagId, dagRunId);

    await this.handleWaitForMultipleOptionsTask(dagId, dagRunId);

    await this.handleWaitForOptionTask(dagId, dagRunId);

    await this.handleApprovalTask(dagId, dagRunId, approve);

    if (approve) {
      await this.handleBranchTask(dagId, dagRunId);
    }

    await this.verifyFinalTaskStates(dagId, dagRunId, approve);
  }

  private async verifyFinalTaskStates(dagId: string, dagRunId: string, approved: boolean): Promise<void> {
    await this.page.goto(`/dags/${dagId}/runs/${dagRunId}`);

    if (approved) {
      await this.waitForTaskState(dagRunId, { expectedState: "Success", taskId: "task_1" });
      await this.waitForTaskState(dagRunId, { expectedState: "Skipped", taskId: "task_2", timeout: 30_000 });
      await this.waitForTaskState(dagRunId, { expectedState: "Skipped", taskId: "task_3", timeout: 30_000 });
    } else {
      await this.waitForTaskState(dagRunId, {
        expectedState: "Skipped",
        taskId: "choose_a_branch_to_run",
        timeout: 30_000,
      });
      await this.waitForTaskState(dagRunId, { expectedState: "Skipped", taskId: "task_1", timeout: 30_000 });
      await this.waitForTaskState(dagRunId, { expectedState: "Skipped", taskId: "task_2", timeout: 30_000 });
      await this.waitForTaskState(dagRunId, { expectedState: "Skipped", taskId: "task_3", timeout: 30_000 });
    }

    await this.navigateToRequiredActionsPage();
    await expect(this.actionsTable).toBeVisible({ timeout: 10_000 });
  }

  private async waitForDagRunState(expectedState: string): Promise<void> {
    await expect(async () => {
      await this.page.reload();
      const detailsPanel = this.page.locator("#details-panel");
      const stateBadge = detailsPanel.getByTestId("state-badge").first();

      await expect(stateBadge).toContainText(expectedState, { timeout: 60_000 });
    }).toPass({ timeout: 180_000 });
  }

  private async waitForTaskState(
    dagRunId: string,
    options: { expectedState: string; taskId: string; timeout?: number },
  ): Promise<void> {
    await expect(async () => {
      await this.page.reload();

      await this.clickOnTaskInGrid(dagRunId, options.taskId);

      const detailsPanel = this.page.locator("#details-panel");
      const stateBadge = detailsPanel.getByTestId("state-badge").first();

      await expect(stateBadge).toContainText(options.expectedState, { timeout: 60_000 });
    }).toPass({ timeout: options.timeout ?? 120_000 });
  }
}
