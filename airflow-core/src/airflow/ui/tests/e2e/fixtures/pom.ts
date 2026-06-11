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

/**
 * Page Object Model fixtures for E2E tests.
 *
 * Provides POM instances and the worker-scoped authenticatedRequest
 * as test fixtures, eliminating manual `beforeEach` boilerplate.
 */
import { test as base, type APIRequestContext } from "@playwright/test";

import { AUTH_FILE, testConfig } from "../../../playwright.config";
import { AssetDetailPage } from "../pages/AssetDetailPage";
import { AssetListPage } from "../pages/AssetListPage";
import { BackfillPage } from "../pages/BackfillPage";
import { ConfigurationPage } from "../pages/ConfigurationPage";
import { ConnectionsPage } from "../pages/ConnectionsPage";
import { DagCalendarTab } from "../pages/DagCalendarTab";
import { DagCodePage } from "../pages/DagCodePage";
import { DagRunsPage } from "../pages/DagRunsPage";
import { DagRunsTabPage } from "../pages/DagRunsTabPage";
import { DagsPage } from "../pages/DagsPage";
import { EventsPage } from "../pages/EventsPage";
import { GridPage } from "../pages/GridPage";
import { HomePage } from "../pages/HomePage";
import { PluginsPage } from "../pages/PluginsPage";
import { PoolsPage } from "../pages/PoolsPage";
import { ProvidersPage } from "../pages/ProvidersPage";
import { RequiredActionsPage } from "../pages/RequiredActionsPage";
import { TaskInstancePage } from "../pages/TaskInstancePage";
import { TaskInstancesPage } from "../pages/TaskInstancesPage";
import { VariablePage } from "../pages/VariablePage";
import { XComsPage } from "../pages/XComsPage";

export type PomWorkerFixtures = {
  authenticatedRequest: APIRequestContext;
};

export type PomFixtures = {
  assetDetailPage: AssetDetailPage;
  assetListPage: AssetListPage;
  backfillPage: BackfillPage;
  configurationPage: ConfigurationPage;
  connectionsPage: ConnectionsPage;
  dagCalendarTab: DagCalendarTab;
  dagCodePage: DagCodePage;
  dagRunsPage: DagRunsPage;
  dagRunsTabPage: DagRunsTabPage;
  dagsPage: DagsPage;
  eventsPage: EventsPage;
  gridPage: GridPage;
  homePage: HomePage;
  pluginsPage: PluginsPage;
  poolsPage: PoolsPage;
  providersPage: ProvidersPage;
  requiredActionsPage: RequiredActionsPage;
  taskInstancePage: TaskInstancePage;
  taskInstancesPage: TaskInstancesPage;
  variablePage: VariablePage;
  xcomsPage: XComsPage;
};

/* eslint-disable react-hooks/rules-of-hooks -- Playwright's `use` is not a React Hook. */
export const test = base.extend<PomFixtures, PomWorkerFixtures>({
  assetDetailPage: async ({ page }, use) => {
    await use(new AssetDetailPage(page));
  },
  assetListPage: async ({ page }, use) => {
    await use(new AssetListPage(page));
  },
  authenticatedRequest: [
    async ({ playwright }, use) => {
      const ctx = await playwright.request.newContext({
        baseURL: testConfig.connection.baseUrl,
        storageState: AUTH_FILE,
      });

      await use(ctx);
      await ctx.dispose();
    },
    { scope: "worker" },
  ],
  backfillPage: async ({ page }, use) => {
    await use(new BackfillPage(page));
  },
  configurationPage: async ({ page }, use) => {
    await use(new ConfigurationPage(page));
  },
  connectionsPage: async ({ page }, use) => {
    await use(new ConnectionsPage(page));
  },
  dagCalendarTab: async ({ page }, use) => {
    await use(new DagCalendarTab(page));
  },
  dagCodePage: async ({ page }, use) => {
    await use(new DagCodePage(page));
  },
  dagRunsPage: async ({ page }, use) => {
    await use(new DagRunsPage(page));
  },
  dagRunsTabPage: async ({ page }, use) => {
    await use(new DagRunsTabPage(page));
  },
  dagsPage: async ({ page }, use) => {
    await use(new DagsPage(page));
  },
  eventsPage: async ({ page }, use) => {
    await use(new EventsPage(page));
  },
  gridPage: async ({ page }, use) => {
    await use(new GridPage(page));
  },
  homePage: async ({ page }, use) => {
    await use(new HomePage(page));
  },
  pluginsPage: async ({ page }, use) => {
    await use(new PluginsPage(page));
  },
  poolsPage: async ({ page }, use) => {
    await use(new PoolsPage(page));
  },
  providersPage: async ({ page }, use) => {
    await use(new ProvidersPage(page));
  },
  requiredActionsPage: async ({ page }, use) => {
    await use(new RequiredActionsPage(page));
  },
  taskInstancePage: async ({ page }, use) => {
    await use(new TaskInstancePage(page));
  },
  taskInstancesPage: async ({ page }, use) => {
    await use(new TaskInstancesPage(page));
  },
  variablePage: async ({ page }, use) => {
    await use(new VariablePage(page));
  },
  xcomsPage: async ({ page }, use) => {
    await use(new XComsPage(page));
  },
});
