import { test, expect } from "@playwright/test";
import { AssetsPage } from "../pages/AssetListPage";

test.describe("Assets Page", () => {
  let assetsPage: AssetsPage;

  test.beforeEach(async ({ page }) => {
    assetsPage = new AssetsPage(page);

    await assetsPage.navigate();
    await assetsPage.waitForAssetsLoad();
  });

  test("should display assets table", async () => {
    await expect(assetsPage.assetsHeading).toBeVisible();
    expect(await assetsPage.hasAssets()).toBe(true);
  });

  test("should display asset names and links", async () => {
    expect(await assetsPage.areAssetLinksVisible()).toBe(true);
  });

  test("should support pagination if multiple pages exist", async () => {
    const initialCount = await assetsPage.getAssetCount();
    expect(initialCount).toBeGreaterThan(0);

    if (await assetsPage.hasNextPage()) {
      await assetsPage.goToNextPage();
      expect(await assetsPage.getAssetCount()).toBeGreaterThan(0);
    }
  });

  test("should filter assets using search", async () => {
    const initialCount = await assetsPage.getAssetCount()
    expect(initialCount).toBeGreaterThan(0)

    await assetsPage.searchAssets("example");

    await expect.poll(
      async () => assetsPage.getAssetCount(),
      { timeout: 30_000 }
    ).toBeLessThanOrEqual(0);
  });
});
