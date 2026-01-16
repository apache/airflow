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

export class TaskInstancePage extends BasePage {
    public readonly logBlock: Locator;
    public readonly downloadButton: Locator;
    public readonly logEntry: Locator;

    public constructor(page: Page) {
        super(page);
        this.logBlock = page.locator('[data-testid="virtualized-list"]');
        // Using a broad match for "Download" in aria-label to be safe against minor copy changes
        this.downloadButton = page.locator('button[aria-label*="Download"]');
        this.logEntry = page.locator('[data-testid^="virtualized-item-"]');
    }

    public static getTaskLogUrl(dagId: string, runId: string, taskId: string): string {
        return `/dags/${dagId}/runs/${runId}/tasks/${taskId}`;
    }

    public async goto(dagId: string, runId: string, taskId: string) {
        await this.page.goto(TaskInstancePage.getTaskLogUrl(dagId, runId, taskId));
        await this.waitForPageLoad();
    }

    public async waitForPageLoad() {
        await expect(this.logBlock).toBeVisible({ timeout: 15000 });
    }

    public async waitForLogsToLoad() {
        // Wait for at least one log item to appear
        await expect(this.logEntry.first()).toBeVisible({ timeout: 20000 });
    }

    public async verifyLogContent(content: string) {
        await expect(this.logBlock).toContainText(content);
    }
}
