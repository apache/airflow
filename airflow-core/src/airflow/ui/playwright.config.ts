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
import { defineConfig, devices } from "@playwright/test";
import path from "node:path";
import { fileURLToPath } from "node:url";

export const testConfig = {
  credentials: {
    password: process.env.TEST_PASSWORD ?? "admin",
    username: process.env.TEST_USERNAME ?? "admin",
  },
  testDag: {
    id: process.env.TEST_DAG_ID ?? "example_bash_operator",
  },
};

const currentFilename = fileURLToPath(import.meta.url);
const currentDirname = path.dirname(currentFilename);

export const AUTH_FILE = path.join(currentDirname, "playwright/.auth/user.json");

export default defineConfig({
  expect: {
    timeout: 5000,
  },
  forbidOnly: process.env.CI !== undefined && process.env.CI !== "",
  fullyParallel: true,
  globalSetup: "./tests/e2e/global-setup.ts",
  projects: [
    {
      name: "chromium",
      use: {
        ...devices["Desktop Chrome"],
        launchOptions: {
          args: [
            "--start-maximized",
            "--disable-web-security",
            "--disable-features=VizDisplayCompositor",
            "--window-size=1920,1080",
            "--window-position=0,0",
          ],
          ignoreDefaultArgs: ["--enable-automation"],
        },
        storageState: AUTH_FILE,
      },
    },
    {
      name: "firefox",
      use: {
        ...devices["Desktop Firefox"],
        launchOptions: {
          args: [
            "--width=1920",
            "--height=1080",
            "--no-sandbox",
            "--disable-dev-shm-usage",
            "--disable-web-security",
          ],
        },
        storageState: AUTH_FILE,
      },
    },
    {
      name: "webkit",
      use: {
        ...devices["Desktop Safari"],
        launchOptions: {
          args: [],
        },
        storageState: AUTH_FILE,
      },
    },
  ],
  reporter: [
    ["html", { outputFolder: "playwright-report" }],
    ["json", { outputFile: "test-results/results.json" }],
    process.env.CI !== undefined && process.env.CI !== "" ? ["github"] : ["list"],
  ],

  retries: process.env.CI !== undefined && process.env.CI !== "" ? 2 : 0,

  testDir: "./tests/e2e/specs",

  timeout: 30_000,
  use: {
    actionTimeout: 10_000,
    baseURL: process.env.AIRFLOW_UI_BASE_URL ?? "http://localhost:28080",
    screenshot: "only-on-failure",
    trace: "on-first-retry",
    video: "retain-on-failure",
    viewport: undefined,
  },

  workers: process.env.CI !== undefined && process.env.CI !== "" ? 2 : undefined,
});
