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

import type { Page, Locator } from "@playwright/test";

export class BasePage {
    readonly page: Page;

    constructor(page: Page) {
        this.page = page;
    }

    async navigateTo(path: string): Promise<void> {
        await this.page.goto(path);
    }

    async waitForElement(selector: string, timeout = 30000): Promise<Locator> {
        return this.page.waitForSelector(selector, { timeout });
    }

    async waitForNavigation(action: () => Promise<void>): Promise<void> {
        await Promise.all([this.page.waitForLoadState("networkidle"), action()]);
    }

    async click(selector: string): Promise<void> {
        await this.page.click(selector);
    }

    async fill(selector: string, value: string): Promise<void> {
        await this.page.fill(selector, value);
    }

    async getText(selector: string): Promise<string> {
        return this.page.textContent(selector) || "";
    }

    async isVisible(selector: string): Promise<boolean> {
        try {
            return await this.page.isVisible(selector);
        } catch {
            return false;
        }
    }

    async exists(selector: string): Promise<boolean> {
        const element = await this.page.$(selector);
        return element !== null;
    }

    async takeScreenshot(name: string): Promise<void> {
        await this.page.screenshot({ path: `screenshots/${name}.png` });
    }

    async wait(ms: number): Promise<void> {
        await this.page.waitForTimeout(ms);
    }
}
