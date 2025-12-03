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
import { expect } from "@playwright/test";
import type { Locator, Page } from "@playwright/test";
import { BasePage } from "tests/e2e/pages/BasePage";

/**
 * Login Page Object
 */
export class LoginPage extends BasePage {
  // Page URLs
  public static get loginUrl(): string {
    return "/auth/login";
  }

  public readonly errorMessage: Locator;
  public readonly loginButton: Locator;
  public readonly passwordInput: Locator;
  public readonly usernameInput: Locator;

  public constructor(page: Page) {
    super(page);

    this.usernameInput = page.locator('input[name="username"]');
    this.passwordInput = page.locator('input[name="password"]');
    // Support both SimpleAuthManager and FabAuthManager login buttons
    this.loginButton = page.locator('button[type="submit"], input[type="submit"]');
    this.errorMessage = page.locator('span:has-text("Invalid credentials")');
  }

  public async expectLoginSuccess(): Promise<void> {
    const currentUrl: string = this.page.url();

    if (currentUrl.includes("/login")) {
      throw new Error(`Expected to be redirected after login, but still on: ${currentUrl}`);
    }

    const isLoggedIn: boolean = await this.isLoggedIn();

    expect(isLoggedIn).toBe(true);
  }
  public async login(username: string, password: string): Promise<void> {
    await this.usernameInput.fill(username);
    await this.passwordInput.fill(password);
    await this.loginButton.click();

    try {
      await this.page.waitForURL(
        (url: URL) => {
          const urlString: string = url.toString();

          return !urlString.includes("/login");
        },
        { timeout: 15_000 },
      );
    } catch (error: unknown) {
      const hasError: boolean = await this.errorMessage.isVisible().catch(() => false);

      if (hasError) {
        throw new Error("Login failed with error message visible");
      }

      throw error;
    }
  }

  public async navigate(): Promise<void> {
    await this.maximizeBrowser();

    await this.navigateTo(LoginPage.loginUrl);
  }

  public async navigateAndLogin(username: string, password: string): Promise<void> {
    await this.navigate();
    await this.login(username, password);
  }
}
