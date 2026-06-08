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
import { expect, test } from "tests/e2e/fixtures";
import { apiCreateVariable, apiDeleteVariable, uniqueRunId } from "tests/e2e/utils/test-helpers";

test.describe("Variables Page", () => {
  test.describe.configure({ mode: "serial" });

  const createdVariables: Array<{
    description: string;
    key: string;
    value: string;
  }> = [];

  const importVarKeys: Array<string> = [];

  test.beforeAll(async ({ authenticatedRequest }) => {
    test.slow();

    const prefix = uniqueRunId("e2e_var");

    for (let i = 0; i < 3; i++) {
      const variable = {
        description: `description_${i}`,
        key: `${prefix}_${i}`,
        value: `value_${i}`,
      };

      createdVariables.push(variable);
      await apiCreateVariable(authenticatedRequest, {
        description: variable.description,
        key: variable.key,
        value: variable.value,
      });
    }
  });

  test.beforeEach(async ({ variablePage }) => {
    await variablePage.navigate();
  });

  test("verify variables table displays", async ({ variablePage }) => {
    await expect(variablePage.table).toBeVisible();
    await expect(variablePage.tableRows).not.toHaveCount(0);
  });

  test("verify search filters", async ({ variablePage }) => {
    const target = createdVariables.at(0);

    expect(target).toBeDefined();

    if (!target) {
      throw new Error("No variables available for test");
    }

    const targetKey = target.key;

    await variablePage.search(targetKey);
    await expect(variablePage.tableRows).toHaveCount(1);
    await expect(variablePage.rowByKey(targetKey)).toBeVisible();
  });

  test("verify editing a variable", async ({ page, variablePage }) => {
    const target = createdVariables.at(1);

    expect(target).toBeDefined();

    if (!target) {
      throw new Error("No variable available for edit test");
    }

    await variablePage.rowByKey(target.key).getByRole("button", { name: /edit/i }).click();

    await expect(page.getByRole("heading", { name: /edit/i })).toBeVisible();

    const descriptionInput = page.getByLabel(/description/i);

    await descriptionInput.fill("updated via e2e");
    await expect(descriptionInput).toHaveValue("updated via e2e");

    await page.getByRole("button", { name: /save/i }).click();

    await variablePage.waitForLoad();

    await expect(variablePage.rowByKey(target.key)).toContainText("updated via e2e");
  });

  test("verify deleting the variable", async ({ page, variablePage }) => {
    const targetKey = uniqueRunId("delete_test");

    const response = await page.request.post("/api/v2/variables", {
      data: {
        description: "to_be_deleted",
        key: targetKey,
        value: "Variable_Deletion",
      },
    });

    expect(response.ok()).toBeTruthy();
    expect(response.status()).toBe(201);

    await variablePage.navigate();

    await variablePage.search(targetKey);
    await expect(variablePage.tableRows).toHaveCount(1);
    await expect(variablePage.rowByKey(targetKey)).toHaveCount(1);

    await variablePage.selectRow(targetKey);
    await page.getByRole("button", { name: /^delete$/i }).click();

    const dialog = page.getByRole("dialog");

    await expect(dialog.getByRole("heading", { name: /delete\s+1\s+variable/i })).toBeVisible();

    const codeBlock = dialog.locator("code");

    await expect(codeBlock).toContainText(targetKey);

    await dialog.getByRole("button", { name: /yes,\s*delete/i }).click();

    await expect(variablePage.rowByKey(targetKey)).toHaveCount(0);
  });

  test("verify importing variables using Import Variables button", async ({ page, variablePage }) => {
    const uniqueId = uniqueRunId("import");

    // Pre-register before UI interactions so afterAll cleans up even on test failure.
    importVarKeys.push(`import_var_${uniqueId}_1`, `import_var_${uniqueId}_2`);

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

    await variablePage.page.getByRole("button", { name: /import variables/i }).click();

    const dialog = page.getByRole("dialog");

    await expect(dialog.getByRole("heading", { name: /import variables/i })).toBeVisible();

    const fileInput = dialog.locator('input[type="file"]');

    await fileInput.setInputFiles({
      buffer: Buffer.from(JSON.stringify(importPayload, undefined, 2)),
      mimeType: "application/json",
      name: "variables.json",
    });

    await dialog.getByRole("button", { name: /import/i }).click();

    await variablePage.waitForLoad();

    for (const key of Object.keys(importPayload)) {
      await expect(variablePage.rowByKey(key)).toHaveCount(1);

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

  test.afterAll(async ({ authenticatedRequest }) => {
    test.slow();

    // Merge all tracked keys: some may not be in createdVariables if a test failed early.
    const allKeys = [...new Set([...createdVariables.map((v) => v.key), ...importVarKeys])];

    if (allKeys.length === 0) {
      return;
    }

    for (const key of allKeys) {
      try {
        await apiDeleteVariable(authenticatedRequest, key);
      } catch {
        // Ignore 404 or other errors - variable may not have been created
      }
    }
  });
});
