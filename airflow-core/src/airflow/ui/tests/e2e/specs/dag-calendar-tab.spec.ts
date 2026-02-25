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
import dayjs from "dayjs";
import { AUTH_FILE, testConfig } from "playwright.config";

import { DagCalendarTab } from "../pages/DagCalendarTab";

test.describe("DAG Calendar Tab", () => {
  test.setTimeout(90_000);
  const dagId = testConfig.testDag.id;
  let calendar: DagCalendarTab;

  test.beforeAll(async ({ browser }) => {
    test.setTimeout(180_000);

    const context = await browser.newContext({ storageState: AUTH_FILE });
    const page = await context.newPage();

    await page.request.patch(`/api/v2/dags/${dagId}`, {
      data: { is_paused: false },
    });

    const now = dayjs();

    const successIso = now.subtract(3, "day").hour(10).toISOString();
    const failedIso = now.subtract(2, "day").hour(12).toISOString();

    async function createRun(runId: string, iso: string, state: string) {
      const response = await page.request.post(`/api/v2/dags/${dagId}/dagRuns`, {
        data: {
          conf: {},
          dag_run_id: runId,
          logical_date: iso,
          note: "e2e test",
        },
      });

      if (!response.ok()) {
        const body = await response.text();

        throw new Error(`Run creation failed: ${response.status()} ${body}`);
      }

      const data = (await response.json()) as { dag_run_id: string };
      const dagRunId = data.dag_run_id;

      await page.request.patch(`/api/v2/dags/${dagId}/dagRuns/${dagRunId}`, { data: { state } });
    }

    await createRun(`cal_success_${Date.now()}`, successIso, "success");
    await createRun(`cal_failed_${Date.now()}`, failedIso, "failed");

    await context.close();
  });

  test.beforeEach(async ({ page }) => {
    test.setTimeout(60_000);
    calendar = new DagCalendarTab(page);
    await calendar.navigateToCalendar(dagId);
  });

  test("verify calendar grid renders", async () => {
    await calendar.switchToHourly();
    await calendar.verifyMonthGridRendered();
  });

  test("verify active cells appear for DAG runs", async () => {
    await calendar.switchToHourly();

    const count = await calendar.getActiveCellCount();

    expect(count).toBeGreaterThan(0);
  });

  test("verify manual runs are detected", async () => {
    await calendar.switchToHourly();

    const states = await calendar.getManualRunStates();

    expect(states.length).toBeGreaterThanOrEqual(2);
  });

  test("verify hover shows correct run states", async () => {
    await calendar.switchToHourly();

    const states = await calendar.getManualRunStates();

    expect(states).toContain("success");
    expect(states).toContain("failed");
  });

  test("failed filter shows only failed runs", async () => {
    await calendar.switchToHourly();

    const totalStates = await calendar.getManualRunStates();

    expect(totalStates).toContain("success");
    expect(totalStates).toContain("failed");

    await calendar.switchToFailedView();

    const failedStates = await calendar.getManualRunStates();

    expect(failedStates).toContain("failed");
    expect(failedStates).not.toContain("success");
  });

  test("failed view reduces active cells", async () => {
    await calendar.switchToHourly();

    const totalCount = await calendar.getActiveCellCount();

    await calendar.switchToFailedView();

    const failedCount = await calendar.getActiveCellCount();

    expect(failedCount).toBeLessThan(totalCount);
  });

  test("color scale changes between total and failed view", async () => {
    await calendar.switchToHourly();

    const totalColors = await calendar.getActiveCellColors();

    await calendar.switchToFailedView();

    const failedColors = await calendar.getActiveCellColors();

    // color palette should differ
    expect(failedColors).not.toEqual(totalColors);
  });

  test("cells reflect failed view mode attribute", async () => {
    await calendar.switchToHourly();
    await calendar.switchToFailedView();

    const cell = calendar.activeCells.first();

    await expect(cell).toHaveAttribute("data-view-mode", "failed");
  });
});
