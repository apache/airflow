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
import { expect, type APIRequestContext, type Locator, type Page } from "@playwright/test";
import { testConfig } from "playwright.config";
import { apiTriggerDagRun, waitForDagReady } from "tests/e2e/utils/test-helpers";

import { BasePage } from "./BasePage";

export class RequiredActionsPage extends BasePage {
  public readonly actionsTable: Locator;
  public readonly emptyStateMessage: Locator;
  public readonly pageHeading: Locator;

  // Standalone API context — page.request degrades after many navigations in WebKit.
  private readonly apiRequest: APIRequestContext;

  public constructor(page: Page, apiRequest?: APIRequestContext) {
    super(page);
    this.apiRequest = apiRequest ?? page.request;
    this.pageHeading = page.getByRole("heading").filter({ hasText: /required action/i });
    this.actionsTable = page.getByTestId("table-list");
    this.emptyStateMessage = page.getByText(/no required actions found/i);
  }

  public static getRequiredActionsUrl(): string {
    return "/required_actions";
  }

  public async getActionsTableRowCount(): Promise<number> {
    const rows = this.actionsTable.locator("tbody").getByRole("row");
    const isTableVisible = await this.actionsTable.isVisible();

    return isTableVisible ? rows.count() : 0;
  }

  public async isEmptyStateDisplayed(): Promise<boolean> {
    return this.emptyStateMessage.isVisible();
  }

  public async isTableDisplayed(): Promise<boolean> {
    return this.actionsTable.isVisible();
  }

  public async navigateToRequiredActionsPage(): Promise<void> {
    await expect(async () => {
      await this.navigateTo(RequiredActionsPage.getRequiredActionsUrl());
      await expect(this.pageHeading).toBeVisible();
    }).toPass({ intervals: [2000], timeout: 60_000 });
  }

  public async runHITLFlowWithApproval(dagId: string): Promise<string> {
    return this.runHITLFlow(dagId, true);
  }

  public async runHITLFlowWithRejection(dagId: string): Promise<string> {
    return this.runHITLFlow(dagId, false);
  }

  private async clickButtonAndWaitForHITLResponse(button: Locator): Promise<void> {
    await expect(async () => {
      const responsePromise = this.page.waitForResponse(
        (res) => res.url().includes("hitlDetails") && res.request().method() === "PATCH",
        { timeout: 30_000 },
      );

      await button.click();
      const response = await responsePromise;

      expect(response.ok(), `HITL PATCH failed with status ${response.status()}`).toBeTruthy();
    }).toPass({ intervals: [3000, 5000], timeout: 60_000 });
  }

  private async clickOnTaskInGrid(dagRunId: string, taskId: string): Promise<void> {
    const taskLocator = this.page.getByTestId(`grid-${dagRunId}-${taskId}`);

    await expect(async () => {
      const gridDataLoaded = this.page.waitForResponse((res) => res.url().includes("/grid") && res.ok(), {
        timeout: 30_000,
      });

      await this.page.reload({ waitUntil: "domcontentloaded" });
      await gridDataLoaded;

      await expect(taskLocator).toBeVisible({ timeout: 20_000 });
      await this.page.mouse.move(0, 0);
      await taskLocator.click({ timeout: 5000 });
    }).toPass({ intervals: [15_000], timeout: 120_000 });
  }

  private async clickRequiredActionLink(): Promise<void> {
    const requiredActionLink = this.page.getByRole("link", { name: /required action/i });

    await expect(requiredActionLink).toBeVisible({ timeout: 30_000 });

    await requiredActionLink.click();

    // Use a specific URL pattern to avoid matching intermediate router states.
    await this.page.waitForURL(/\/dags\/.*\/runs\/.*\/tasks\//, { timeout: 30_000 });
  }

  private async handleApprovalTask(dagId: string, dagRunId: string, approve: boolean): Promise<void> {
    await this.waitForTaskState(dagId, dagRunId, {
      expectedState: "Deferred",
      taskId: "valid_input_and_options",
    });

    await this.clickRequiredActionLink();

    const buttonName = approve ? "Approve" : "Reject";
    const actionButton = this.page.getByTestId(`hitl-option-${buttonName}`);

    await expect(actionButton).toBeVisible({ timeout: 30_000 });

    const informationInput = this.page.getByRole("textbox");

    if (await informationInput.isVisible()) {
      await informationInput.fill("Approved by test");
    }

    await expect(actionButton).toBeEnabled();
    await this.clickButtonAndWaitForHITLResponse(actionButton);

    await this.page.goto(`/dags/${dagId}/runs/${dagRunId}`);
    await this.waitForTaskState(dagId, dagRunId, {
      expectedState: "Success",
      taskId: "valid_input_and_options",
    });
  }

  private async handleBranchTask(dagId: string, dagRunId: string): Promise<void> {
    await this.waitForTaskState(dagId, dagRunId, {
      expectedState: "Deferred",
      taskId: "choose_a_branch_to_run",
    });

    await this.clickRequiredActionLink();

    const branchButton = this.page.getByTestId("hitl-option-task_1");

    await expect(branchButton).toBeVisible({ timeout: 30_000 });
    await this.clickButtonAndWaitForHITLResponse(branchButton);

    await this.page.goto(`/dags/${dagId}/runs/${dagRunId}`);
    await this.waitForTaskState(dagId, dagRunId, {
      expectedState: "Success",
      taskId: "choose_a_branch_to_run",
    });
  }

  private async handleWaitForInputTask(dagId: string, dagRunId: string): Promise<void> {
    await this.waitForTaskState(dagId, dagRunId, { expectedState: "Deferred", taskId: "wait_for_input" });

    await this.clickRequiredActionLink();

    const informationInput = this.page.getByRole("textbox");

    await expect(informationInput).toBeVisible({ timeout: 30_000 });
    await informationInput.fill("test");

    const okButton = this.page.getByRole("button", { name: "OK" });

    await expect(okButton).toBeVisible();
    await this.clickButtonAndWaitForHITLResponse(okButton);

    await this.page.goto(`/dags/${dagId}/runs/${dagRunId}`);
    await this.waitForTaskState(dagId, dagRunId, { expectedState: "Success", taskId: "wait_for_input" });
  }

  private async handleWaitForMultipleOptionsTask(dagId: string, dagRunId: string): Promise<void> {
    await this.waitForTaskState(dagId, dagRunId, {
      expectedState: "Deferred",
      taskId: "wait_for_multiple_options",
    });

    const multiSelectContainer = this.page.getByRole("combobox", { name: /select one or multiple values/i });

    await expect(async () => {
      await this.clickRequiredActionLink();
      await expect(multiSelectContainer).toBeVisible({ timeout: 15_000 });
    }).toPass({ intervals: [5000], timeout: 60_000 });

    await multiSelectContainer.click();

    await this.page.getByRole("option", { name: "option 4" }).click();
    await multiSelectContainer.click();
    await this.page.getByRole("option", { name: "option 5" }).click();

    const respondButton = this.page.getByRole("button", { name: "Respond" });

    await expect(respondButton).toBeVisible();
    await this.clickButtonAndWaitForHITLResponse(respondButton);

    await this.page.goto(`/dags/${dagId}/runs/${dagRunId}`);
    await this.waitForTaskState(dagId, dagRunId, {
      expectedState: "Success",
      taskId: "wait_for_multiple_options",
    });
  }

  private async handleWaitForOptionTask(dagId: string, dagRunId: string): Promise<void> {
    await this.waitForTaskState(dagId, dagRunId, { expectedState: "Deferred", taskId: "wait_for_option" });

    await this.clickRequiredActionLink();

    const optionButton = this.page.getByTestId("hitl-option-option 1");

    await expect(optionButton).toBeVisible({ timeout: 30_000 });
    await this.clickButtonAndWaitForHITLResponse(optionButton);

    await this.page.goto(`/dags/${dagId}/runs/${dagRunId}`);
    await this.waitForTaskState(dagId, dagRunId, { expectedState: "Success", taskId: "wait_for_option" });
  }

  private async runHITLFlow(dagId: string, approve: boolean): Promise<string> {
    const { baseUrl } = testConfig.connection;

    await waitForDagReady(this.apiRequest, dagId);
    await this.apiRequest.patch(`${baseUrl}/api/v2/dags/${dagId}`, {
      data: { is_paused: false },
    });
    const { dagRunId } = await apiTriggerDagRun(this.apiRequest, dagId);

    await this.waitForDagRunState(dagId, dagRunId, "Running");

    await this.waitForTaskState(dagId, dagRunId, {
      expectedState: "Success",
      taskId: "wait_for_default_option",
      timeout: 120_000,
    });

    await this.handleWaitForInputTask(dagId, dagRunId);

    await this.handleWaitForMultipleOptionsTask(dagId, dagRunId);

    await this.handleWaitForOptionTask(dagId, dagRunId);

    await this.handleApprovalTask(dagId, dagRunId, approve);

    if (approve) {
      await this.handleBranchTask(dagId, dagRunId);
    }

    await this.verifyFinalTaskStates(dagId, dagRunId, approve);

    return dagRunId;
  }

  private async verifyFinalTaskStates(dagId: string, dagRunId: string, approved: boolean): Promise<void> {
    await this.page.goto(`/dags/${dagId}/runs/${dagRunId}`);

    if (approved) {
      await this.waitForTaskState(dagId, dagRunId, { expectedState: "Success", taskId: "task_1" });
      await this.waitForTaskState(dagId, dagRunId, {
        expectedState: "Skipped",
        taskId: "task_2",
        timeout: 30_000,
      });
      await this.waitForTaskState(dagId, dagRunId, {
        expectedState: "Skipped",
        taskId: "task_3",
        timeout: 30_000,
      });
    } else {
      await this.waitForTaskState(dagId, dagRunId, {
        expectedState: "Skipped",
        taskId: "choose_a_branch_to_run",
        timeout: 30_000,
      });
      await this.waitForTaskState(dagId, dagRunId, {
        expectedState: "Skipped",
        taskId: "task_1",
        timeout: 30_000,
      });
      await this.waitForTaskState(dagId, dagRunId, {
        expectedState: "Skipped",
        taskId: "task_2",
        timeout: 30_000,
      });
      await this.waitForTaskState(dagId, dagRunId, {
        expectedState: "Skipped",
        taskId: "task_3",
        timeout: 30_000,
      });
    }

    await this.navigateToRequiredActionsPage();
    await expect(this.actionsTable).toBeVisible();
  }

  private async waitForDagRunState(dagId: string, runId: string, expectedState: string): Promise<void> {
    const { baseUrl } = testConfig.connection;

    await expect
      .poll(
        async () => {
          try {
            const response = await this.apiRequest.get(`${baseUrl}/api/v2/dags/${dagId}/dagRuns/${runId}`, {
              timeout: 30_000,
            });

            if (!response.ok()) {
              return "unknown";
            }

            const data = (await response.json()) as { state: string };

            return data.state;
          } catch {
            // Transient network/timeout errors — let expect.poll retry.
            return "unknown";
          }
        },
        {
          intervals: [5000],
          message: `Dag run ${runId} did not reach state "${expectedState}"`,
          timeout: 120_000,
        },
      )
      .toBe(expectedState.toLowerCase());

    // toPass handles transient states between API response and page render.
    await this.page.goto(`/dags/${dagId}/runs/${runId}`);
    const detailsPanel = this.page.locator("#details-panel");
    const stateBadge = detailsPanel.getByTestId("state-badge").first();

    await expect(async () => {
      await expect(stateBadge).toBeVisible({ timeout: 5000 });
      await expect(stateBadge).toContainText(new RegExp(expectedState, "i"), { timeout: 5000 });
    }).toPass({ intervals: [2000], timeout: 30_000 });
  }

  private async waitForTaskState(
    dagId: string,
    dagRunId: string,
    options: { expectedState: string; taskId: string; timeout?: number },
  ): Promise<void> {
    const { baseUrl } = testConfig.connection;

    const terminalStates = new Set(["success", "failed", "skipped", "removed", "upstream_failed"]);

    await expect
      .poll(
        async () => {
          try {
            const response = await this.apiRequest.get(
              `${baseUrl}/api/v2/dags/${dagId}/dagRuns/${dagRunId}/taskInstances/${options.taskId}`,
              { timeout: 30_000 },
            );

            if (!response.ok()) {
              return "unknown";
            }

            const data = (await response.json()) as { state: string };
            const { state } = data;
            const expected = options.expectedState.toLowerCase();

            if (state !== expected && terminalStates.has(state)) {
              throw new Error(
                `Task ${options.taskId} reached terminal state "${state}" instead of "${expected}"`,
              );
            }

            return state;
          } catch (error) {
            if (error instanceof Error && error.message.includes("terminal state")) {
              throw error;
            }

            return "unknown";
          }
        },
        {
          intervals: [5000],
          message: `Task ${options.taskId} did not reach state "${options.expectedState}"`,
          timeout: options.timeout ?? 120_000,
        },
      )
      .toBe(options.expectedState.toLowerCase());

    await this.clickOnTaskInGrid(dagRunId, options.taskId);

    const detailsPanel = this.page.locator("#details-panel");
    const stateBadge = detailsPanel.getByTestId("state-badge").first();

    await expect(async () => {
      await expect(stateBadge).toBeVisible({ timeout: 5000 });
      await expect(stateBadge).toContainText(new RegExp(options.expectedState, "i"), { timeout: 5000 });
    }).toPass({ intervals: [2000], timeout: 30_000 });
  }
}
