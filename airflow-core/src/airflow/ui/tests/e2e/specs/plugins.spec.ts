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

  test("verify plugins list has at least one entry", async () => {
    await expect(pluginsPage.rows).not.toHaveCount(0);
  });

  test("verify each plugin has a name", async () => {
    await expect(pluginsPage.rows).not.toHaveCount(0);
    const count = await pluginsPage.rows.count();

    for (let i = 0; i < count; i++) {
      await expect(pluginsPage.nameColumn.nth(i)).not.toBeEmpty();
    }
  });

  test("verify each plugin has a source", async () => {
    await expect(pluginsPage.rows).not.toHaveCount(0);
    const count = await pluginsPage.rows.count();

    for (let i = 0; i < count; i++) {
      await expect(pluginsPage.sourceColumn.nth(i)).not.toBeEmpty();
    }
  });

  test("verify plugin names and sources have matching counts", async () => {
    const rowCount = await pluginsPage.rows.count();

    await expect(pluginsPage.nameColumn).toHaveCount(rowCount);
    await expect(pluginsPage.sourceColumn).toHaveCount(rowCount);
  });
});
