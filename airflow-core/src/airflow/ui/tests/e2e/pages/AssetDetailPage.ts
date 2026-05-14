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

export class AssetDetailPage extends BasePage {
  public static get url(): string {
    return "/assets";
  }

  public get assetOnlyButton(): Locator {
    return this.page.getByRole("button", { name: /^Asset Only$/ });
  }

  public get fullLineageButton(): Locator {
    return this.page.getByRole("button", { name: /^Full$/ });
  }

  public get graphViewport(): Locator {
    return this.page.locator(".react-flow__viewport");
  }

  public get lineageSearchInput(): Locator {
    return this.page.getByPlaceholder("Search lineage nodes");
  }

  public constructor(page: Page) {
    super(page);
  }

  public async clickOnAsset(name: string): Promise<void> {
    const responsePromise = this.page.waitForResponse(
      (res) => /\/api\/v2\/assets\/\d+(\?|$)/.test(res.url()) && res.ok(),
      { timeout: 15_000 },
    );

    await this.page.getByRole("link", { exact: true, name }).click();
    await responsePromise;
  }

  public getHeading(name: string): Locator {
    return this.page.getByRole("heading", { name });
  }

  public async getViewportTransform(): Promise<string> {
    return this.graphViewport.evaluate((element) => getComputedStyle(element).transform);
  }

  public async goto(): Promise<void> {
    await this.navigateTo(AssetDetailPage.url);
  }

  public async gotoMockAsset(assetId = 1): Promise<void> {
    await this.navigateTo(`/assets/${assetId}?mockAssets=true`);
  }

  public graphNode(name: string): Locator {
    return this.page.locator(".react-flow__node").filter({
      hasText: name,
    });
  }

  public async searchLineage(term: string): Promise<void> {
    await this.lineageSearchInput.fill(term);
  }

  public async switchToAssetOnly(): Promise<void> {
    await this.assetOnlyButton.click();
  }

  public async verifyAssetDetails(name: string): Promise<void> {
    await expect(this.page.getByRole("heading", { name })).toBeVisible();
  }
  public async verifyProducingTasks(): Promise<void> {
    await this.verifyStatSection("Producing Tasks");
  }

  public async verifyScheduledDags(): Promise<void> {
    await this.verifyStatSection("Scheduled Dags");
  }

  /**
   * Common helper to verify stat sections (Producing Tasks, Scheduled Dags)
   * Uses stable selectors based on text content and ARIA roles
   */
  private async verifyStatSection(labelText: string): Promise<void> {
    const label = this.page.getByRole("heading", { exact: true, name: labelText });

    await expect(label).toBeVisible();

    // Find the button that follows the label in the same stat section
    // Navigate to the label's parent container and find the first button within it
    const statContainer = label.locator("xpath=./parent::*");
    const button = statContainer.getByRole("button").first();

    await expect(button).toBeVisible();
    await expect(button).toBeEnabled();
    await expect(button).toHaveText(/^[1-9]/);

    const text = await button.textContent();
    const count = parseInt(text?.split(" ")[0] ?? "0", 10);

    await button.click();
    await expect(button).toHaveAttribute("aria-expanded", "true", { timeout: 5000 });
    const popoverLinks = this.page.getByRole("dialog").last().getByRole("link");

    await expect(popoverLinks).toHaveCount(count);
    await button.click();
    await expect(button).toHaveAttribute("aria-expanded", "false", { timeout: 5000 });
  }
}
