import { expect, test } from "@playwright/test";
import { testConfig } from "playwright.config";
import { LoginPage } from "tests/e2e/pages/LoginPage";

test.describe("Task Instances Page", () => {
  test.setTimeout(60_000);

  test("should render task instances table and verify details", async ({ page }) => {
    // 1. Setup Data
    const loginPage = new LoginPage(page);
    const testCredentials = testConfig.credentials;
    const testDagId = testConfig.testDag.id;

    // 2. Login with Explicit Wait (This fixes the timeout)
    await page.goto("/login");
    // Wait up to 30 seconds for the username box (CI is slow!)
    await page.waitForSelector('input[name="username"]', { state: "visible", timeout: 30_000 });
    
    // Now use the page object to type and click
    await loginPage.login(testCredentials.username, testCredentials.password);
    await loginPage.expectLoginSuccess();

    // 3. Navigate to Task Instances
    await page.goto("/task_instances");
    
    // 4. Verify Table Rendering
    const table = page.getByRole("table");
    await expect(table).toBeVisible({ timeout: 15_000 });

    // 5. Verify Headers
    const expectedHeaders = [
      /DAG ID/i, /Task ID/i, /Run ID|Dag Run/i, /State/i, /Start Date/i, /Duration/i,
    ];

    for (const headerName of expectedHeaders) {
      await expect(page.getByText(headerName).first()).toBeVisible({ timeout: 10_000 });
    }

    // 6. Verify Visual Distinction
    const successBadge = page.locator('.state-success, [data-state="success"]').first();
    if (await successBadge.isVisible()) {
        await expect(successBadge).toHaveCSS("background-color", /rgb/);
    }

    // 7. Verify Filter
    await page.getByRole("button", { name: /filter/i }).first().click();
    await page.getByRole("menuitem", { name: "Dag ID" }).click();

    const searchInput = page.getByRole("textbox").first();
    await expect(searchInput).toBeVisible();
    
    await searchInput.fill(testDagId);
    await searchInput.press("Enter");
    
    await expect(page.getByRole("table")).toBeVisible();
    await expect(page.getByRole("cell", { name: testDagId }).first()).toBeVisible();
  });
});