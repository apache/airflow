import type { Locator, Page } from "@playwright/test";
import { BasePage } from "./BasePage";

export class AssetsPage extends BasePage {
  readonly assetsHeading: Locator;
  readonly tableRows: Locator;
  readonly nextPageButton: Locator;
  readonly searchInput: Locator;

  constructor(page: Page) {
    super(page);

    this.assetsHeading = page.getByRole("heading", { name: /asset/i });
    this.tableRows = page.locator("tbody tr");
    this.nextPageButton = page.getByTestId("next");
    this.searchInput = page.getByPlaceholder(/search/i);
  }

  async navigate(): Promise<void> {
    await this.navigateTo("/assets");
  }

  async waitForAssetsLoad(): Promise<void> {
    await this.assetsHeading.waitFor({ state: "visible", timeout: 30_000 });
  }

  async hasAssets(): Promise<boolean> {
    return (await this.tableRows.count()) > 0;
  }

  async getAssetCount(): Promise<number> {
    return this.tableRows.count();
  }

  async areAssetLinksVisible(): Promise<boolean> {
    const firstRowLink = this.tableRows.first().locator("a");
    return firstRowLink.isVisible();
  }

  async hasNextPage(): Promise<boolean> {
    return this.nextPageButton.isVisible();
  }

  async goToNextPage(): Promise<void> {
    if (await this.hasNextPage()) {
      await this.nextPageButton.click();
    }
  }

  async searchAssets(value: string): Promise<void> {
    await this.searchInput.fill(value);
  }
}
