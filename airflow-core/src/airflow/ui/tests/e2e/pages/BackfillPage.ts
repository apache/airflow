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
import { expect } from "@playwright/test";
import type { Locator, Page } from "@playwright/test";
import { testConfig } from "playwright.config";
import { BasePage } from "tests/e2e/pages/BasePage";
import {
  apiCancelAllActiveBackfills,
  apiCancelBackfill,
  apiWaitForBackfillComplete,
  apiWaitForNoActiveBackfill,
} from "tests/e2e/utils/test-helpers";

export const REPROCESS_API_TO_UI = {
  completed: "All Runs",
  failed: "Missing and Errored Runs",
  none: "Missing Runs",
} as const;

export type ReprocessBehaviorApi = keyof typeof REPROCESS_API_TO_UI;

type BackfillDetails = {
  completedAt: string;
  createdAt: string;
  fromDate: string;
  reprocessBehavior: string;
  toDate: string;
};

type BackfillRowIdentifier = {
  fromDate: string;
  toDate: string;
};

type CreateBackfillOptions = {
  fromDate: string;
  maxActiveRuns?: number;
  reprocessBehavior?: ReprocessBehaviorApi;
  toDate: string;
};

type BackfillApiResponse = {
  completed_at: string | null;
  id: number;
};

const {
  connection: { baseUrl },
} = testConfig;

function getColumnIndex(columnMap: Map<string, number>, name: string): number {
  const index = columnMap.get(name);

  if (index === undefined) {
    const available = [...columnMap.keys()].join(", ");

    throw new Error(`Column "${name}" not found. Available columns: ${available}`);
  }

  return index;
}

export class BackfillPage extends BasePage {
  public readonly backfillDateError: Locator;
  public readonly backfillFromDateInput: Locator;
  public readonly backfillModeRadio: Locator;
  public readonly backfillRunButton: Locator;
  public readonly backfillsTable: Locator;
  public readonly backfillToDateInput: Locator;
  public readonly cancelButton: Locator;
  public readonly pauseButton: Locator;
  public readonly triggerButton: Locator;
  public readonly unpauseButton: Locator;

  public get pauseOrUnpauseButton(): Locator {
    return this.pauseButton.or(this.unpauseButton);
  }

  public constructor(page: Page) {
    super(page);
    this.triggerButton = page.getByTestId("trigger-dag-button");
    // Chakra UI radio cards: target the label directly since <input> is hidden.
    this.backfillModeRadio = page.locator("label").getByText("Backfill", { exact: true });
    this.backfillFromDateInput = page.getByTestId("datetime-input").first();
    this.backfillToDateInput = page.getByTestId("datetime-input").nth(1);
    this.backfillRunButton = page.getByRole("button", { name: "Run Backfill" });
    this.backfillsTable = page.getByTestId("table-list");
    this.backfillDateError = page.getByText("Start Date must be before the End Date");
    this.cancelButton = page.getByRole("button", { name: "Cancel backfill" });
    this.pauseButton = page.getByRole("button", { name: "Pause backfill" });
    this.unpauseButton = page.getByRole("button", { name: "Unpause backfill" });
  }

  public static getBackfillsUrl(dagName: string): string {
    return `/dags/${dagName}/backfills`;
  }

  public static getDagDetailUrl(dagName: string): string {
    return `/dags/${dagName}`;
  }

  public async cancelAllActiveBackfillsViaApi(dagId: string): Promise<void> {
    await apiCancelAllActiveBackfills(this.page, dagId);
  }

  public async cancelBackfillViaApi(backfillId: number): Promise<void> {
    await apiCancelBackfill(this.page, backfillId);
  }

  public async clickCancelButton(): Promise<void> {
    await this.cancelButton.click();
    await expect(this.cancelButton).not.toBeVisible({ timeout: 15_000 });
  }

  /** Create a backfill through the UI dialog. Returns the backfill ID. Caller must ensure no active backfills exist. */
  public async createBackfill(dagName: string, options: CreateBackfillOptions): Promise<number> {
    const { fromDate, reprocessBehavior = "none", toDate } = options;

    const uiFromDate = fromDate.slice(0, 16);
    const uiToDate = toDate.slice(0, 16);

    await this.navigateToDagDetail(dagName);
    await this.openBackfillDialog();

    await this.backfillFromDateInput.click();
    await this.backfillFromDateInput.fill(uiFromDate);
    await this.backfillFromDateInput.press("Tab");

    await this.backfillToDateInput.click();
    await this.backfillToDateInput.fill(uiToDate);
    await this.backfillToDateInput.press("Tab");

    await this.selectReprocessBehavior(reprocessBehavior);

    const runsWillBeTriggered = this.page.getByText(/\d+ runs? will be triggered/);
    const noRunsMatching = this.page.getByText(/No runs matching/);

    await expect(runsWillBeTriggered.or(noRunsMatching)).toBeVisible({ timeout: 20_000 });

    if (await noRunsMatching.isVisible()) {
      await this.page.keyboard.press("Escape");

      throw new Error(
        `No runs matching: dag=${dagName}, from=${fromDate}, to=${toDate}, reprocess=${reprocessBehavior}`,
      );
    }

    await expect(this.backfillRunButton).toBeEnabled();

    const responsePromise = this.page.waitForResponse(
      (res) =>
        res.url().includes("/backfills") &&
        !res.url().includes("/dry_run") &&
        res.request().method() === "POST",
      { timeout: 30_000 },
    );

    await this.backfillRunButton.click();

    const apiResponse = await responsePromise;
    const status = apiResponse.status();

    if (status < 200 || status >= 300) {
      const body = await apiResponse.text().catch(() => "unknown");

      await this.page.keyboard.press("Escape");

      throw new Error(`Backfill creation failed with status ${status}: ${body}`);
    }

    const data = (await apiResponse.json()) as BackfillApiResponse;

    return data.id;
  }

  /** Create a backfill via API. On 409, cancels active backfills and retries once. */
  public async createBackfillViaApi(dagId: string, options: CreateBackfillOptions): Promise<number> {
    const { fromDate, maxActiveRuns, reprocessBehavior = "none", toDate } = options;

    const body: Record<string, unknown> = {
      dag_id: dagId,
      from_date: fromDate,
      reprocess_behavior: reprocessBehavior,
      to_date: toDate,
    };

    if (maxActiveRuns !== undefined) {
      body.max_active_runs = maxActiveRuns;
    }

    const response = await this.page.request.post(`${baseUrl}/api/v2/backfills`, {
      data: body,
      headers: { "Content-Type": "application/json" },
      timeout: 30_000,
    });

    if (response.status() === 409) {
      await this.cancelAllActiveBackfillsViaApi(dagId);
      await this.waitForNoActiveBackfillViaApi(dagId, 30_000);
      const retryResponse = await this.page.request.post(`${baseUrl}/api/v2/backfills`, {
        data: body,
        headers: { "Content-Type": "application/json" },
        timeout: 30_000,
      });

      expect(retryResponse.ok()).toBe(true);

      return ((await retryResponse.json()) as BackfillApiResponse).id;
    }

    expect(response.ok()).toBe(true);

    return ((await response.json()) as BackfillApiResponse).id;
  }

  /**
   * Create a backfill and immediately pause it. Retries the full create+pause
   * cycle up to 3 times to handle the race where the scheduler completes the
   * backfill before the pause call lands.
   */
  public async createPausedBackfillViaApi(dagId: string, options: CreateBackfillOptions): Promise<number> {
    for (let attempt = 0; attempt < 3; attempt++) {
      const backfillId = await this.createBackfillViaApi(dagId, {
        ...options,
        maxActiveRuns: 1,
      });

      const paused = await this.pauseBackfillViaApi(backfillId);

      if (paused) {
        return backfillId;
      }

      // Backfill completed before we could pause — cancel and retry.
      await this.cancelAllActiveBackfillsViaApi(dagId);
      await this.waitForNoActiveBackfillViaApi(dagId, 30_000);
    }

    throw new Error(`Failed to create a paused backfill for ${dagId} after 3 attempts`);
  }

  public async findBackfillRowByDateRange(
    identifier: BackfillRowIdentifier,
    timeout: number = 120_000,
  ): Promise<{ columnMap: Map<string, number>; row: Locator }> {
    const { fromDate: expectedFrom, toDate: expectedTo } = identifier;

    let foundRow: Locator | undefined;
    let foundColumnMap: Map<string, number> | undefined;

    await expect
      .poll(
        async () => {
          try {
            if (!(await this.backfillsTable.isVisible())) {
              return false;
            }

            const headers = this.backfillsTable.locator("thead th");

            if (!(await headers.first().isVisible())) {
              return false;
            }

            const headerTexts = await headers.allTextContents();
            const columnMap = new Map(headerTexts.map((text, index) => [text.trim(), index]));
            const fromIndex = columnMap.get("From");
            const toIndex = columnMap.get("To");

            if (fromIndex === undefined || toIndex === undefined) {
              return false;
            }

            const rows = this.backfillsTable.locator("tbody tr");
            const rowCount = await rows.count();

            for (let i = 0; i < rowCount; i++) {
              const row = rows.nth(i);
              const cells = row.locator("td");
              const fromCell = ((await cells.nth(fromIndex).textContent()) ?? "").slice(0, 10);
              const toCell = ((await cells.nth(toIndex).textContent()) ?? "").slice(0, 10);

              if (fromCell === expectedFrom.slice(0, 10) && toCell === expectedTo.slice(0, 10)) {
                foundRow = row;
                foundColumnMap = columnMap;

                return true;
              }
            }

            return false;
          } catch (error: unknown) {
            console.warn("findBackfillRowByDateRange poll error:", error);

            return false;
          }
        },
        {
          intervals: [2000, 5000],
          message: `Backfill row with dates ${expectedFrom} ~ ${expectedTo} not found in table`,
          timeout,
        },
      )
      .toBeTruthy();

    if (!foundRow || !foundColumnMap) {
      throw new Error(`Backfill row with dates ${expectedFrom} ~ ${expectedTo} not found`);
    }

    return { columnMap: foundColumnMap, row: foundRow };
  }

  public async getBackfillDetailsByDateRange(identifier: BackfillRowIdentifier): Promise<BackfillDetails> {
    const { columnMap, row } = await this.findBackfillRowByDateRange(identifier);
    const cells = row.locator("td");

    const fromIndex = getColumnIndex(columnMap, "From");
    const toIndex = getColumnIndex(columnMap, "To");
    const reprocessIndex = getColumnIndex(columnMap, "Reprocess Behavior");
    const createdAtIndex = getColumnIndex(columnMap, "Created at");
    const completedAtIndex = getColumnIndex(columnMap, "Completed at");

    const [fromDate, toDate, reprocessBehavior, createdAt, completedAt] = await Promise.all([
      cells.nth(fromIndex).textContent(),
      cells.nth(toIndex).textContent(),
      cells.nth(reprocessIndex).textContent(),
      cells.nth(createdAtIndex).textContent(),
      cells.nth(completedAtIndex).textContent(),
    ]);

    return {
      completedAt: (completedAt ?? "").trim(),
      createdAt: (createdAt ?? "").trim(),
      fromDate: (fromDate ?? "").trim(),
      reprocessBehavior: (reprocessBehavior ?? "").trim(),
      toDate: (toDate ?? "").trim(),
    };
  }

  public getColumnHeader(columnName: string): Locator {
    return this.backfillsTable.getByRole("columnheader", { name: columnName });
  }

  public getFilterButton(): Locator {
    return this.page.getByRole("button", { name: /filter table columns/i });
  }

  public async navigateToBackfillsTab(dagName: string): Promise<void> {
    await expect(async () => {
      await this.navigateTo(BackfillPage.getBackfillsUrl(dagName));
      await expect(this.backfillsTable).toBeVisible({ timeout: 5000 });
    }).toPass({ intervals: [2000], timeout: 60_000 });
  }

  public async navigateToDagDetail(dagName: string): Promise<void> {
    await expect(async () => {
      await this.navigateTo(BackfillPage.getDagDetailUrl(dagName));
      await expect(this.triggerButton).toBeVisible({ timeout: 5000 });
    }).toPass({ intervals: [2000], timeout: 60_000 });
  }

  public async openBackfillDialog(): Promise<void> {
    await this.triggerButton.click({ timeout: 15_000 });
    await this.backfillModeRadio.click();
    await expect(this.backfillFromDateInput).toBeVisible();
  }

  public async openFilterMenu(): Promise<void> {
    await expect(async () => {
      if (!(await this.page.getByRole("menu").isVisible())) {
        await this.getFilterButton().click();
      }
      await expect(this.page.getByRole("menu")).toBeVisible({ timeout: 3000 });
      await this.page.getByRole("menuitem").first().click({ timeout: 3000, trial: true });
    }).toPass({ intervals: [1000], timeout: 15_000 });
  }

  public async pauseBackfillViaApi(backfillId: number): Promise<boolean> {
    let isPaused = false;

    try {
      // Retry: the server may not have fully initialized the backfill yet.
      await expect
        .poll(
          async () => {
            const response = await this.page.request.put(`${baseUrl}/api/v2/backfills/${backfillId}/pause`, {
              timeout: 30_000,
            });

            if (response.ok()) {
              isPaused = true;

              return true;
            }

            // 409 means the backfill already completed — not retriable.
            if (response.status() === 409) {
              isPaused = false;

              return true;
            }

            return false;
          },
          {
            intervals: [2000],
            message: `Failed to pause backfill ${backfillId}`,
          },
        )
        .toBeTruthy();
    } catch {
      return false;
    }

    return isPaused;
  }

  public async selectReprocessBehavior(behavior: ReprocessBehaviorApi): Promise<void> {
    const label = REPROCESS_API_TO_UI[behavior];

    await this.page
      .getByRole("radiogroup", { name: "Reprocess Behavior" })
      .locator("label")
      .filter({ hasText: label })
      .click();
  }

  public async toggleColumn(columnName: string): Promise<void> {
    await this.page.getByRole("menuitem", { name: columnName }).click();
  }

  public async togglePauseState(): Promise<void> {
    await this.pauseOrUnpauseButton.click();
  }

  public async waitForBackfillComplete(backfillId: number, timeout: number = 120_000): Promise<void> {
    await apiWaitForBackfillComplete(this.page, backfillId, timeout);
  }

  public async waitForNoActiveBackfillViaApi(dagId: string, timeout: number = 120_000): Promise<void> {
    await apiWaitForNoActiveBackfill(this.page, dagId, timeout);
  }
}
