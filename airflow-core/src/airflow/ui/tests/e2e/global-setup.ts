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
import { chromium, firefox, webkit, type FullConfig } from "@playwright/test";
import fs from "node:fs";
import path from "node:path";

import { AUTH_FILE, testConfig } from "../../playwright.config";
import { LoginPage } from "./pages/LoginPage";

const browsers = { chromium, firefox, webkit };

/**
 * Authenticate once before all tests and save state for reuse
 */
async function globalSetup(config: FullConfig) {
  const [firstProject] = config.projects as [FullConfig["projects"][number]];
  const baseURL = firstProject.use.baseURL ?? "http://localhost:28080";
  const { password, username } = testConfig.credentials;
  const browserName = firstProject.name as keyof typeof browsers;
  const browserType = browsers[browserName];

  const authDir = path.dirname(AUTH_FILE);

  if (!fs.existsSync(authDir)) {
    fs.mkdirSync(authDir, { recursive: true });
  }

  const browser = await browserType.launch();
  const context = await browser.newContext({ baseURL });
  const page = await context.newPage();

  try {
    const loginPage = new LoginPage(page);

    await loginPage.navigateAndLogin(username, password);
    await context.storageState({ path: AUTH_FILE });
  } finally {
    await browser.close();
  }
}

export default globalSetup;
