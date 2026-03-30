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

export class DagCodePage extends BasePage {
  public readonly editorContainer: Locator;
  public readonly editorScrollable: Locator;
  public readonly lineNumbers: Locator;
  public readonly syntaxTokens: Locator;
  public readonly viewLines: Locator;

  public constructor(page: Page) {
    super(page);
    this.editorContainer = page.locator('[role="code"]');
    this.lineNumbers = page.locator(".monaco-editor .line-numbers");
    this.editorScrollable = page.locator(".monaco-scrollable-element");
    this.syntaxTokens = page.locator(".monaco-editor .view-line span span");
    this.viewLines = page.locator(".monaco-editor .view-line");
  }

  public async navigateToCodeTab(dagId: string): Promise<void> {
    await expect(async () => {
      await this.navigateTo(`/dags/${dagId}/code`);
      await expect(this.editorContainer).toBeVisible({ timeout: 10_000 });
    }).toPass({ intervals: [2000], timeout: 60_000 });
    await this.waitForCodeReady();
  }

  public async verifyCodeIsScrollable(): Promise<void> {
    await this.waitForCodeReady();

    const scrollable = this.editorScrollable.first();

    await expect(scrollable).toBeVisible({ timeout: 30_000 });

    await expect
      .poll(async () => scrollable.evaluate((el) => el.scrollHeight > el.clientHeight), {
        timeout: 10_000,
      })
      .toBe(true);
  }

  public async verifyLineNumbersDisplayed(): Promise<void> {
    await this.waitForCodeReady();

    await expect(this.lineNumbers.first()).toBeVisible({ timeout: 30_000 });
    await expect(this.lineNumbers).not.toHaveCount(0);
    await expect(this.lineNumbers.first()).toHaveText("1");
  }

  public async verifySourceCodeDisplayed(): Promise<void> {
    await this.waitForCodeReady();

    await expect(this.viewLines.first()).toBeVisible({ timeout: 30_000 });
    await expect(this.viewLines).not.toHaveCount(0);
  }

  public async verifySyntaxHighlighting(): Promise<void> {
    await this.waitForCodeReady();

    await expect(this.syntaxTokens.first()).toBeVisible({ timeout: 30_000 });
    await expect(this.syntaxTokens).not.toHaveCount(0);
    await expect(this.syntaxTokens.first()).toHaveAttribute("class", /mtk\d+/);
  }

  private async waitForCodeReady(): Promise<void> {
    await expect(this.editorContainer).toBeVisible({ timeout: 60_000 });
    await expect(this.viewLines.first()).toBeVisible({ timeout: 30_000 });
  }
}
