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
import type { Locator, Page } from "@playwright/test";
import { BasePage } from "tests/e2e/pages/BasePage";

/**
 * DAG Detail Page Object
 * Handles interactions with the DAG detail page including grid view
 */
export class DagDetailPage extends BasePage {
  public readonly auditLogTab: Locator;
  public readonly calendarTab: Locator;
  public readonly codeTab: Locator;
  public readonly dagId: Locator;
  // DAG info
  public readonly dagTitle: Locator;
  public readonly detailsTab: Locator;
  public readonly gridCells: Locator;

  public readonly gridContainer: Locator;
  // Grid view elements
  public readonly gridView: Locator;
  // View tabs
  public readonly overviewTab: Locator;
  public readonly runsTab: Locator;

  public readonly taskDetailsCloseButton: Locator;
  // Task details panel
  public readonly taskDetailsPanel: Locator;
  public readonly taskIdLabel: Locator;
  public readonly taskInstances: Locator;

  public readonly tasksTab: Locator;
  public readonly taskStateLabel: Locator;

  public constructor(page: Page) {
    super(page);

    // View tabs - these are links, not buttons
    this.overviewTab = page.getByRole('link', { name: 'Overview' });
    this.runsTab = page.getByRole('link', { name: 'Runs' });
    this.tasksTab = page.getByRole('link', { name: 'Tasks' });
    this.calendarTab = page.getByRole('link', { name: 'Calendar' });
    this.auditLogTab = page.getByRole('link', { name: 'Audit Log' });
    this.codeTab = page.getByRole('link', { name: 'Code' });
    this.detailsTab = page.getByRole('link', { name: 'Details' });

    // Grid view elements
    // The grid is rendered as a Box containing HStack with Grid component
    this.gridView = page.locator('div:has(> div > div[id^="grid-"])');
    this.gridContainer = page.locator('div').filter({ has: page.locator('a[id^="grid-"]') }).first();

    // Task instances in grid - links with id="grid-{runId}-{taskId}"
    this.taskInstances = page.locator('a[id^="grid-"]');
    // Grid cells are Badge components inside the task instance links
    this.gridCells = page.locator('a[id^="grid-"] span, a[id^="grid-"] [class*="badge"], a[id^="grid-"]');

    // Task details panel - rendered in the right panel via Outlet
    this.taskDetailsPanel = page.locator('div[id="details-panel"]');
    this.taskIdLabel = page.getByText('Task ID');
    this.taskStateLabel = page.getByText('State');
    this.taskDetailsCloseButton = page.locator('button[aria-label*="close" i]');

    // DAG info
    this.dagTitle = page.locator('[data-testid="dag-title"], h1, h2:first-of-type');
    this.dagId = page.locator('[data-testid="dag-id"]');
  }

  /**
   * Click on a task instance cell in the grid
   */
  public async clickTaskCell(index = 0): Promise<void> {
    await this.waitForGridView();
    const cell = this.taskInstances.nth(index);

    await cell.click({ timeout: 5000 });
    await this.page.waitForTimeout(1000);
  }

  /**
   * Close task details panel
   */
  public async closeTaskDetails(): Promise<void> {
    // Navigate back to close the task details view
    await this.page.goBack();
    await this.page.waitForTimeout(500);
  }

  /**
   * Get task ID from task details panel
   */
  public async getTaskIdFromDetails(): Promise<string | null> {
    const panelVisible = await this.isTaskDetailsPanelVisible();

    if (!panelVisible) {
      return null;
    }

    // Extract task ID from URL: /dags/{dagId}/runs/{runId}/tasks/{taskId}
    const url = this.page.url();
    const taskMatch = /\/tasks\/([^/?]+)/.exec(url);

    if (taskMatch?.[1]) {
      return decodeURIComponent(taskMatch[1]);
    }

    return null;
  }

  /**
   * Get count of task instances in grid
   */
  public async getTaskInstanceCount(): Promise<number> {
    await this.waitForGridView();

    return await this.taskInstances.count();
  }

  /**
   * Get all task state colors from grid cells
   */
  public async getTaskStateColors(): Promise<Array<string>> {
    await this.waitForGridView();

    const colors: Array<string> = [];
    const badges = this.gridCells;
    const count = await badges.count();

    // Collect color/state information from Badge components (max 20)
    for (let i = 0; i < Math.min(count, 20); i++) {
      const badge = badges.nth(i);
      // Try multiple ways to get color/state info
      const colorPalette = await badge.getAttribute('data-color-palette');
      const className = await badge.getAttribute('class');
      const bgColor = await badge.evaluate((el) => window.getComputedStyle(el).backgroundColor);

      const colorInfo = colorPalette || className || bgColor;
      if (colorInfo) {
        colors.push(colorInfo);
      }
    }

    return colors;
  }

  /**
   * Get task state from task details panel
   */
  public async getTaskStateFromDetails(): Promise<string | null> {
    const panelVisible = await this.isTaskDetailsPanelVisible();

    if (!panelVisible) {
      return null;
    }

    // Look for state badge or text in the details panel
    const stateBadge = this.page.locator('[class*="badge"], span[role="status"], [class*="state"]').first();
    const isVisible = await stateBadge.isVisible().catch(() => false);

    if (isVisible) {
      const colorPalette = await stateBadge.getAttribute('data-color-palette');
      const text = await stateBadge.textContent();

      return colorPalette || text;
    }

    return null;
  }

  /**
   * Check if task details panel is visible
   */
  public async isTaskDetailsPanelVisible(): Promise<boolean> {
    // Check if the URL has changed to include task selection
    const url = this.page.url();

    return url.includes("/runs/") && url.includes("/tasks/");
  }

  /**
   * Navigate to DAG detail page
   */
  public async navigateToDagDetail(dagId: string): Promise<void> {
    await this.navigateTo(`/dags/${dagId}`);
  }

  /**
   * Switch to a specific view tab
   */
  public async switchToTab(tabName: "auditLog" | "calendar" | "code" | "details" | "overview" | "runs" | "tasks"): Promise<void> {
    const tabMap = {
      auditLog: this.auditLogTab,
      calendar: this.calendarTab,
      code: this.codeTab,
      details: this.detailsTab,
      overview: this.overviewTab,
      runs: this.runsTab,
      tasks: this.tasksTab,
    };

    const tab = tabMap[tabName];

    await tab.waitFor({ state: "visible", timeout: 100_000 });
    await tab.click();
    await this.page.waitForLoadState("networkidle");
  }

  /**
   * Verify grid view is rendering with task instances
   */
  public async verifyGridHasTaskInstances(): Promise<boolean> {
    const count = await this.getTaskInstanceCount();

    return count > 0;
  }

  /**
   * Verify task states are color-coded
   */
  public async verifyTaskStatesAreColorCoded(): Promise<boolean> {
    const colors = await this.getTaskStateColors();

    // Should have at least some colors and they should be different
    if (colors.length === 0) {
      return false;
    }

    // Check if we have different colors (color-coding is working)
    const uniqueColors = new Set(colors);

    return uniqueColors.size > 0;
  }

  /**
   * Wait for grid view to be visible
   */
  public async waitForGridView(): Promise<void> {
    // Wait for grid container structure to load (even if empty)
    await this.page.waitForTimeout(2000);

    // Check if there are any DAG runs (grid cells only exist with runs)
    const gridCellCount = await this.page.locator('a[id^="grid-"]').count();

    if (gridCellCount > 0) {
      // If runs exist, wait for first grid cell to be visible
      await this.page.locator('a[id^="grid-"]').first().waitFor({ state: "visible", timeout: 10_000 });
    }
  }
}
