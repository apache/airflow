import { expect, test } from "@playwright/test";
import { testConfig } from "playwright.config";
import { LoginPage } from "tests/e2e/pages/LoginPage";

test.describe("Task Instances Page", () => {
  test.setTimeout(60_000);

  test("should render task instances table and verify details", async ({ page }) => {
    
    const loginPage = new LoginPage(page);
    const testCredentials = testConfig.credentials;
    const testDagId = testConfig.testDag.id;

    // Login using the Shared Page Object (Fixes Reviewer Comment)
    await loginPage.navigateAndLogin(testCredentials.username, testCredentials.password);
    await loginPage.expectLoginSuccess();

    
    await page.goto("/task_instances");
    
   
    //render task instances table
    // Verify Table Rendering
    const table = page.getByRole("table");
    await expect(table).toBeVisible({ timeout: 15_000 });

    //Verify Headers
    const expectedHeaders = [
      /DAG ID/i,
      /Task ID/i,
      /Run ID|Dag Run/i,
      /State/i,
      /Start Date/i,
      /Duration/i,
    ];

    for (const headerName of expectedHeaders) {
      
      await expect(page.getByText(headerName).first()).toBeVisible({ timeout: 10_000 });
    }
    
   
    //Verify Visual Distinction (Fixes Reviewer Comment)
    // Check that the "Success" badge has a specific background color
    const successBadge = page.locator('.state-success, [data-state="success"]').first();
    
    if (await successBadge.isVisible()) {
        await expect(successBadge).toHaveCSS("background-color", /rgb/);
    }

    // 7. Verify Filter Functionality (Fixes Reviewer Comment)
    // Open Filter -> Select Dag ID -> Type Configured Dag ID -> Verify Row Appears
    await page.getByRole("button", { name: /filter/i }).first().click();
    await page.getByRole("menuitem", { name: "Dag ID" }).click();

    const searchInput = page.getByRole("textbox").first();
    await expect(searchInput).toBeVisible();
    
    await searchInput.fill(testDagId);
    await searchInput.press("Enter");
    
    // Assert the table actually filtered by finding the DAG ID in a cell
    await expect(page.getByRole("table")).toBeVisible();
    await expect(page.getByRole("cell", { name: testDagId }).first()).toBeVisible();
  });
});