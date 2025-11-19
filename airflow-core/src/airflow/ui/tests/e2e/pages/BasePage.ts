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
import type { Page, Locator } from '@playwright/test';

/**
 * Base Page Object
 */
export class BasePage {
  readonly page: Page;
  readonly welcomeHeading: Locator;

  constructor(page: Page) {
    this.page = page;
    this.welcomeHeading = page.locator('h2.chakra-heading:has-text("Welcome")');
  }

  async maximizeBrowser(): Promise<void> {
    try {
      await this.page.setViewportSize({ width: 1920, height: 1080 });
    } catch (error) {
      // Viewport size could not be set
    }
  }

  async waitForPageLoad(): Promise<void> {
    await this.page.waitForLoadState('networkidle');
  }

  async navigateTo(path: string): Promise<void> {
    await this.page.goto(path);
    await this.waitForPageLoad();
  }

  async isLoggedIn(): Promise<boolean> {
    try {
      await this.welcomeHeading.waitFor({ timeout: 30000 });
      return true;
    } catch {
      const currentUrl = this.page.url();
      return !currentUrl.includes('/login');
    }
  }
}
