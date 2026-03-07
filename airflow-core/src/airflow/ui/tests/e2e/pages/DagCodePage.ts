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

  public constructor(page: Page) {
    super(page);
    this.editorContainer = page.locator('[role="code"]');
    this.lineNumbers = page.locator(".monaco-editor .line-numbers");
    this.editorScrollable = page.locator(".monaco-scrollable-element");
    this.syntaxTokens = page.locator(".monaco-editor .view-line span span");
  }

  public async navigateToCodeTab(dagId: string): Promise<void> {
    await this.navigateTo(`/dags/${dagId}/code`);
    await this.waitForCodeReady();
  }
  public async verifyCodeIsScrollable(): Promise<void> {
    await this.waitForCodeReady();

    const scrollable = this.editorScrollable.first();

    await expect(scrollable).toBeVisible({ timeout: 30_000 });

    // For a sufficiently long file the scroll-height exceeds the client-height
    const isScrollable = await scrollable.evaluate((el) => el.scrollHeight > el.clientHeight);

    expect(isScrollable).toBe(true);
  }

  public async verifyLineNumbersDisplayed(): Promise<void> {
    await this.waitForCodeReady();

    await expect(this.lineNumbers.first()).toBeVisible({ timeout: 30_000 });

    const lineNumberCount = await this.lineNumbers.count();

    expect(lineNumberCount).toBeGreaterThan(0);

    const firstLineText = await this.lineNumbers.first().textContent();

    expect(firstLineText?.trim()).toBe("1");
  }

  public async verifySourceCodeDisplayed(): Promise<void> {
    await this.waitForCodeReady();

    const viewLines = this.page.locator(".monaco-editor .view-line");

    await expect(viewLines.first()).toBeVisible({ timeout: 30_000 });

    const lineCount = await viewLines.count();

    expect(lineCount).toBeGreaterThan(0);
  }

  public async verifySyntaxHighlighting(): Promise<void> {
    await this.waitForCodeReady();

    const tokens = this.syntaxTokens;

    await expect(tokens.first()).toBeVisible({ timeout: 30_000 });

    const tokenCount = await tokens.count();

    expect(tokenCount).toBeGreaterThan(1);

    const classNames = await tokens.first().getAttribute("class");

    expect(classNames).toMatch(/mtk\d+/);
  }

  private async waitForCodeReady(): Promise<void> {
    await this.editorContainer.waitFor({ state: "visible", timeout: 60_000 });

    const viewLines = this.page.locator(".monaco-editor .view-line");

    await viewLines.first().waitFor({ state: "visible", timeout: 30_000 });
  }
}
