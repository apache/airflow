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

import type { Page } from "@playwright/test";

import { BasePage } from "./BasePage";
import { testConfig } from "../testConfig";

export class LoginPage extends BasePage {
    constructor(page: Page) {
        super(page);
    }

    async goto(): Promise<void> {
        await this.navigateTo(testConfig.paths.login);
    }

    async login(
        username: string = testConfig.username,
        password: string = testConfig.password,
    ): Promise<void> {
        // Navigate to login page
        await this.goto();

        // Check if already logged in
        const isLoggedIn = await this.isVisible('a[href="/logout"]');
        if (isLoggedIn) {
            return;
        }

        // Fill login form
        await this.fill('input[name="username"]', username);
        await this.fill('input[name="password"]', password);

        // Submit and wait for navigation
        await this.waitForNavigation(async () => {
            await this.click('button[type="submit"]');
        });
    }

    async isOnLoginPage(): Promise<boolean> {
        return this.exists('input[name="username"]');
    }
}
