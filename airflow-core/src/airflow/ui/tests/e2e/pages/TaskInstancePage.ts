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

export class TaskInstancePage extends BasePage {
  public readonly confirmTriggerButton: Locator;
  public readonly stateBadge: Locator;
  public readonly triggerButton: Locator;

  public constructor(page: Page) {
    super(page);
    this.triggerButton = page.locator('button[aria-label="Trigger Dag"]:has-text("Trigger")');
    this.confirmTriggerButton = page.locator('button:has-text("Trigger")').last();
    this.stateBadge = page.getByTestId("state-badge").first();
  }

  public async navigateToDag(dagId: string): Promise<void> {
    await this.navigateTo(`/dags/${dagId}`);
  }

  public async navigateToTaskInstance(dagId: string, runId: string, taskId: string): Promise<void> {
    await this.navigateTo(`/dags/${dagId}/runs/${runId}/tasks/${taskId}`);
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
