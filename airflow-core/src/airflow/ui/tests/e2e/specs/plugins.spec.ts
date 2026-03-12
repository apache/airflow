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
import { expect, test } from "@playwright/test";

import { PluginsPage } from "../pages/PluginsPage";

test.describe("Plugins Page", () => {
  let pluginsPage: PluginsPage;

  test.beforeEach(async ({ page }) => {
    pluginsPage = new PluginsPage(page);
    await pluginsPage.navigate();
    await pluginsPage.waitForLoad();
  });

  test("verify plugins page heading is visible", async () => {
    await expect(pluginsPage.heading).toBeVisible();
  });

  test("verify plugins table is visible", async () => {
    await expect(pluginsPage.table).toBeVisible();
  });

  test("verify plugins list displays with data", async () => {
    const count = await pluginsPage.getPluginCount();

    expect(count).toBeGreaterThan(0);
  });

  test("verify each plugin has a name", async () => {
    const pluginNames = await pluginsPage.getPluginNames();

    expect(pluginNames.length).toBeGreaterThan(0);

    for (const name of pluginNames) {
      expect(name.trim().length).toBeGreaterThan(0);
    }
  });

  test("verify each plugin has a source", async () => {
    const pluginSources = await pluginsPage.getPluginSources();

    expect(pluginSources.length).toBeGreaterThan(0);

    for (const source of pluginSources) {
      expect(source.trim().length).toBeGreaterThan(0);
    }
  });

  test("verify plugin names and sources have matching counts", async () => {
    const pluginNames = await pluginsPage.getPluginNames();
    const pluginSources = await pluginsPage.getPluginSources();

    expect(pluginNames.length).toBe(pluginSources.length);
  });
});
