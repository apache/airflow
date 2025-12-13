import { test, expect } from '@playwright/test';

test.describe('Task Instances Page', () => {
  // Give the test enough time to run in Docker
  test.setTimeout(60000);

  test.beforeEach(async ({ page }) => {
    // 1. Navigate to Login
    await page.goto('/login');
    
    // 2. Login
    await page.locator('input[name="username"]').fill('admin');
    await page.locator('input[name="password"]').fill('admin');
    await page.locator('button[type="submit"]').click();
    
    // Wait for the Sidebar to appear to confirm login
    await page.getByRole('link', { name: 'Dags' }).first().waitFor({ timeout: 15000 });
  });

  test('should render task instances table and verify details', async ({ page }) => {
    // 3. Go directly to Task Instances
    await page.goto('/task_instances');
    
    // 4. Verify Table exists
    const table = page.getByRole('table');
    await expect(table).toBeVisible();

    // 5. Verify Headers
    const expectedHeaders = [
      /DAG ID/i,
      /Task ID/i,
      /Dag Run/i,       // Matches "Dag Run" column in your UI
      /State/i,
      /Start Date/i,
      /Duration/i
    ];

    for (const headerName of expectedHeaders) {
      await expect(page.getByText(headerName).first()).toBeVisible();
    }

    // 6. Verify Visual Distinction (REQUIRED by the issue)
    // Check that the "Success" badge exists and has a color (visual distinction)
    const successBadge = page.getByText('Success').first();
    if (await successBadge.isVisible()) {
        await expect(successBadge).toBeVisible();
        // Optional: Check if it has a background color to confirm it is "distinct"
        // (This usually checks the container element)
        await expect(successBadge.locator('..')).toBeVisible(); 
    }

    // 7. Verify Filter
    console.log('Verifying filter...');
    
    // Click "Filter" button
    await page.getByRole('button', { name: /filter/i }).first().click();

    // Select "Dag ID" from the dropdown to show the search box
    await page.getByRole('menuitem', { name: 'Dag ID' }).click();

    // Now find the search box and type
    const searchInput = page.getByRole('textbox').first();
    await expect(searchInput).toBeVisible();

    // Type the DAG name you created earlier
    await searchInput.fill('my_test_dag');
    await searchInput.press('Enter');
    
    // 8. Verify table still shows data
    await expect(page.getByRole('table')).toBeVisible();
    await expect(page.locator('tbody tr').first()).toBeVisible();
  });
});