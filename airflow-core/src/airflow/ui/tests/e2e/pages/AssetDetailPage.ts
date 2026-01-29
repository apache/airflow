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
import { expect, type Page } from "@playwright/test";

import { BasePage } from "./BasePage";

export class AssetDetailPage extends BasePage {
  public static get url(): string {
    return "/assets";
  }

  public constructor(page: Page) {
    super(page);
  }

  public async clickOnAsset(name: string): Promise<void> {
    await this.page.getByRole("link", { exact: true, name }).click();
  }

  public async goto(): Promise<void> {
    await this.navigateTo(AssetDetailPage.url);
  }

  public async verifyAssetDetails(name: string): Promise<void> {
    await expect(this.page.getByRole("heading", { name })).toBeVisible();
  }

  public async verifyProducingTasks(minCount: number): Promise<void> {
    await this.verifyStatSection("Producing Tasks", minCount);
  }

  public async verifyScheduledDags(minCount: number): Promise<void> {
    await this.verifyStatSection("Scheduled Dags", minCount);
  }

  /**
   * Common helper to verify stat sections (Producing Tasks, Scheduled Dags)
   * Uses stable selectors based on text content and ARIA roles
   */
  private async verifyStatSection(labelText: string, minCount: number): Promise<void> {
    const label = this.page.getByText(labelText, { exact: true });

    await expect(label).toBeVisible();

    // Find the button that follows the label in the same stat section
    // Navigate to the label's parent container and find the first button within it
    const statContainer = label.locator("xpath=./parent::*");
    const button = statContainer.getByRole("button").first();

    await expect(button).toBeVisible();
    const text = await button.textContent();
    const count = parseInt(text?.split(" ")[0] ?? "0", 10);

    expect(count).toBeGreaterThanOrEqual(minCount);

    if (count > 0) {
      await button.click();
      await expect(button).toHaveAttribute("aria-expanded", "true", { timeout: 5000 });
      const popoverLinks = this.page.getByRole("dialog").last().getByRole("link");

      await expect(popoverLinks).toHaveCount(count);
      await button.click();
      await expect(button).toHaveAttribute("aria-expanded", "false", { timeout: 5000 });
    }
  }
}
