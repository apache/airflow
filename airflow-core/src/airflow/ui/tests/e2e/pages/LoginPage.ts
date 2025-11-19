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
import { expect } from '@playwright/test';
import type { Locator } from '@playwright/test';

import { BasePage } from './BasePage';

/**
 * Login Page Object
 */
export class LoginPage extends BasePage {
  readonly usernameInput: Locator;
  readonly passwordInput: Locator;
  readonly loginButton: Locator;
  readonly errorMessage: Locator;

  // Page URLs
  readonly loginUrl = '/auth/login';

  constructor(page: any) {
    super(page);

    this.usernameInput = page.locator('input[name="username"]');
    this.passwordInput = page.locator('input[name="password"]');
    this.loginButton = page.locator('button[type="submit"]');
    this.errorMessage = page.locator('span:has-text("Invalid credentials")');
  }

  async navigate(): Promise<void> {
    await this.maximizeBrowser();
    await this.navigateTo(this.loginUrl);
  }
  async navigateAndLogin(username: string, password: string): Promise<void> {
    await this.navigate();
    await this.login(username, password);
  }

  async login(username: string, password: string): Promise<void> {
    await this.usernameInput.fill(username);
    await this.passwordInput.fill(password);
    await this.loginButton.click();

    try {
      await this.page.waitForURL(url => !url.toString().includes('/login'), { timeout: 15000 });
    } catch (error) {
      const hasError = await this.errorMessage.isVisible().catch(() => false);
      if (hasError) {
        throw new Error('Login failed with error message visible');
      }
      throw error;
    }
  }


  async expectLoginSuccess(): Promise<void> {
    const currentUrl = this.page.url();
    if (currentUrl.includes('/login')) {
      throw new Error(`Expected to be redirected after login, but still on: ${currentUrl}`);
    }
    await expect(this.isLoggedIn()).resolves.toBe(true);
  }

}
