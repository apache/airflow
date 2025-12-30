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

export class AssetsPage extends BasePage {
  public static get url(): string {
    return "/assets";
  }

  public readonly assetsTable: Locator;

  public constructor(page: Page) {
    super(page);
    this.assetsTable = page.locator("table");
  }

  public async clickOnAsset(name: string): Promise<void> {
    await this.page.getByRole("link", { exact: true, name }).click();
  }

  public async goto(): Promise<void> {
    await this.navigateTo(AssetsPage.url);
  }

  public async verifyAssetDetails(name: string): Promise<void> {
    await expect(this.page.getByRole("heading", { name })).toBeVisible();
  }

  public async verifyProducingTasks(minCount: number): Promise<void> {
    // Find the stat box for Producing Tasks
    // We look for the label "Producing Tasks"
    const label = this.page.getByText("Producing Tasks", { exact: true });

    await expect(label).toBeVisible();

    // The value (button) is typically a sibling or in the same container.
    // In HeaderCard -> Stat, layout is usually vertical or horizontal.
    // We can look for the button in the vicinity.
    // The button text will be like "1 Task" or "2 Tasks"

    // Strategy: Get the parent of the label, then find the button inside it.
    // Assuming Stat component structure.
    const statBox = label.locator("..");
    const button = statBox.getByRole("button");

    await expect(button).toBeVisible();
    const text = await button.textContent();
    const count = parseInt(text?.split(" ")[0] ?? "0", 10);

    expect(count).toBeGreaterThanOrEqual(minCount);

    if (count > 0) {
      await button.click();
      // Verify popover content (links)
      await expect(this.page.locator(".chakra-popover__body a")).toHaveCount(count);
      // Close popover
      await this.page.keyboard.press("Escape");
    }
  }

  public async verifyScheduledDags(minCount: number): Promise<void> {
    const label = this.page.getByText("Scheduled Dags", { exact: true });

    await expect(label).toBeVisible();

    const statBox = label.locator("..");
    const button = statBox.getByRole("button");

    await expect(button).toBeVisible();
    const text = await button.textContent();
    const count = parseInt(text?.split(" ")[0] ?? "0", 10);

    expect(count).toBeGreaterThanOrEqual(minCount);

    if (count > 0) {
      await button.click();
      await expect(this.page.locator(".chakra-popover__body a")).toHaveCount(count);
      await this.page.keyboard.press("Escape");
    }
  }
}
