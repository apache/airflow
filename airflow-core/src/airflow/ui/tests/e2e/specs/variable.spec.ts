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
import { test, expect } from "@playwright/test";
import { AUTH_FILE } from "playwright.config";

import { VariablePage } from "../pages/VariablePage";

test.describe("Variables Page", () => {
  let variablesPage: VariablePage;
  let createVariables = 3;

  const createdVariables: Array<{
    description: string;
    key: string;
    value: string;
  }> = [];

  test.beforeAll(async ({ browser }) => {
    test.setTimeout(420_000); // for slower browsers
    const context = await browser.newContext({ storageState: AUTH_FILE });
    const page = await context.newPage();

    variablesPage = new VariablePage(page);

    await variablesPage.navigate();

    for (let i = 0; i < createVariables; i++) {
      const variable = {
        description: `description_${i}`,
        key: `e2e_var_${Date.now()}_${i}_${Math.random().toString(36).slice(2, 8)}`,
        value: `value_${i}`,
      };

      createdVariables.push(variable);

      // Wait for dialog backdrop to fully disappear
      await expect(page.locator('[data-part="backdrop"]')).toHaveCount(0);

      await variablesPage.addButton.click();

      await expect(page.getByRole("heading", { name: /add/i })).toBeVisible({ timeout: 20_000 });

      await page.getByLabel(/key/i).fill(variable.key);
      await page.getByLabel(/value/i).fill(variable.value);

      if (variable.description) {
        await page.getByLabel(/description/i).fill(variable.description);
      }

      await page.getByRole("button", { name: /save/i }).click();

      await expect(variablesPage.rowByKey(variable.key)).toHaveCount(1);
    }

    await context.close();
  });

  test.beforeEach(async ({ page }) => {
    variablesPage = new VariablePage(page);
    await variablesPage.navigate();
    await variablesPage.waitForLoad();
  });

  test("verify variables table displays", async () => {
    await expect(variablesPage.table).toBeVisible();

    const rowCount = await variablesPage.tableRows.count();

    expect(rowCount).toBeGreaterThan(0);
  });

  test("verify search filters", async () => {
    const target = createdVariables.at(0);

    expect(target).toBeDefined();

    if (!target) {
      throw new Error("No variables available for test");
    }

    const targetKey = target.key;

    await variablesPage.search(targetKey);
    await expect(variablesPage.tableRows).toHaveCount(1);
    await expect(variablesPage.rowByKey(targetKey)).toBeVisible();
  });

  test("verify editing a variable", async ({ page }) => {
    const target = createdVariables.at(1);

    expect(target).toBeDefined();

    if (!target) {
      throw new Error("No variable available for edit test");
    }

    await variablesPage.rowByKey(target.key).getByRole("button", { name: /edit/i }).click();

    await expect(page.getByRole("heading", { name: /edit/i })).toBeVisible();

    await page.getByLabel(/description/i).fill("updated via e2e");
    await page.getByRole("button", { name: /save/i }).click();

    await variablesPage.waitForLoad();

    await expect(variablesPage.rowByKey(target.key)).toContainText("updated via e2e");
  });

  test("verify deleting the variable", async ({ page }) => {
    const targetKey = `delete_test_${Date.now()}`;

    const response = await page.request.post("/api/v2/variables", {
      data: {
        description: "to_be_deleted",
        key: targetKey,
        value: "Variable_Deletion",
      },
    });

    expect(response.ok()).toBeTruthy();
    expect(response.status()).toBe(201);

    await variablesPage.navigate();
    await variablesPage.waitForLoad();

    await variablesPage.search(targetKey);
    await expect(variablesPage.tableRows).toHaveCount(1, { timeout: 10_000 });
    await expect(variablesPage.rowByKey(targetKey)).toHaveCount(1, { timeout: 10_000 });

    await variablesPage.selectRow(targetKey);
    await page.getByRole("button", { name: /^delete$/i }).click();

    const dialog = page.getByRole("dialog");

    await expect(dialog.getByRole("heading", { name: /delete\s+1\s+variable/i })).toBeVisible();

    const codeBlock = dialog.locator("code");

    await expect(codeBlock).toContainText(targetKey);

    await dialog.getByRole("button", { name: /yes,\s*delete/i }).click();

    await expect(variablesPage.rowByKey(targetKey)).toHaveCount(0);
  });

  test("verify importing variables using Import Variables button", async ({ page }) => {
    const uniqueId = `${Date.now()}_${Math.random().toString(36).slice(2, 6)}`;

    const importPayload = {
      [`import_var_${uniqueId}_1`]: {
        description: "imported via e2e 1",
        value: "imported_value_1",
      },
      [`import_var_${uniqueId}_2`]: {
        description: "imported via e2e 2",
        value: "imported_value_2",
      },
    };

    await variablesPage.page.getByRole("button", { name: /import variables/i }).click();

    const dialog = page.getByRole("dialog");

    await expect(dialog.getByRole("heading", { name: /import variables/i })).toBeVisible();

    const fileInput = dialog.locator('input[type="file"]');

    await fileInput.setInputFiles({
      buffer: Buffer.from(JSON.stringify(importPayload, undefined, 2)),
      mimeType: "application/json",
      name: "variables.json",
    });

    await dialog.getByRole("button", { name: /import/i }).click();

    await variablesPage.waitForLoad();

    for (const key of Object.keys(importPayload)) {
      await expect(variablesPage.rowByKey(key)).toHaveCount(1);

      const variable = importPayload[key];

      if (!variable) {
        throw new Error(`Missing import payload for key: ${key}`);
      }

      createdVariables.push({
        description: variable.description,
        key,
        value: variable.value,
      });
    }
  });

  test.afterAll(async ({ browser }) => {
    test.setTimeout(300_000);
    if (createdVariables.length === 0) {
      return;
    }

    const context = await browser.newContext({ storageState: AUTH_FILE });
    const page = await context.newPage();

    variablesPage = new VariablePage(page);

    await variablesPage.navigate();
    await variablesPage.waitForLoad();

    const keysToDelete: Array<string> = [];

    for (const variable of createdVariables) {
      const row = variablesPage.rowByKey(variable.key);

      if ((await row.count()) > 0) {
        await variablesPage.selectRow(variable.key);
        keysToDelete.push(variable.key);
      }
    }

    if (keysToDelete.length === 0) {
      await context.close();

      return;
    }

    await page.getByRole("button", { name: /^delete$/i }).click();

    const dialog = page.getByRole("dialog");

    await expect(
      dialog.getByRole("heading", {
        name: new RegExp(`delete\\s+${keysToDelete.length}\\s+variable`, "i"),
      }),
    ).toBeVisible();

    const codeBlock = dialog.locator("code");

    for (const key of keysToDelete) {
      await expect(codeBlock).toContainText(key);
    }

    await dialog.getByRole("button", { name: /yes,\s*delete/i }).click();

    for (const key of keysToDelete) {
      await expect(variablesPage.rowByKey(key)).toHaveCount(0);
    }

    await context.close();
  });
});
