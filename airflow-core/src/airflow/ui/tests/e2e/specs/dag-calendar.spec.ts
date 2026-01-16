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
import { AUTH_FILE, testConfig } from "playwright.config";
import { DagCalendarPage } from "tests/e2e/pages/DagCalendarPage";

test.describe("DAG Calendar Page", () => {
    test.setTimeout(60_000);

    let dagCalendarPage: DagCalendarPage;
    const testDagId = testConfig.testDag.id;

    test.beforeAll(async ({ browser }) => {
        const context = await browser.newContext({ storageState: AUTH_FILE });
        const page = await context.newPage();
        const baseUrl = process.env.AIRFLOW_UI_BASE_URL ?? "http://localhost:8080";

        const timestamp = Date.now();
        const date = new Date(timestamp);
        const todayDateString = date.toISOString().split("T")[0]; // YYYY-MM-DD

        // Trigger DAG runs to ensure data
        // Success Run
        const runId1 = `test_run_cal_success_${timestamp}`;
        const logicalDate1 = new Date(timestamp).toISOString();
        const triggerResponse1 = await page.request.post(`${baseUrl}/api/v2/dags/${testDagId}/dagRuns`, {
            data: JSON.stringify({
                dag_run_id: runId1,
                logical_date: logicalDate1,
            }),
            headers: { "Content-Type": "application/json" },
        });
        expect(triggerResponse1.ok()).toBeTruthy();

        const runData1 = (await triggerResponse1.json()) as { dag_run_id: string };
        await page.request.patch(
            `${baseUrl}/api/v2/dags/${testDagId}/dagRuns/${runData1.dag_run_id}`,
            {
                data: JSON.stringify({ state: "success" }),
                headers: { "Content-Type": "application/json" },
            }
        );

        // Failed Run
        const runId2 = `test_run_cal_failed_${timestamp}`;
        const logicalDate2 = new Date(timestamp + 60000).toISOString();
        const triggerResponse2 = await page.request.post(`${baseUrl}/api/v2/dags/${testDagId}/dagRuns`, {
            data: JSON.stringify({
                dag_run_id: runId2,
                logical_date: logicalDate2,
            }),
            headers: { "Content-Type": "application/json" },
        });
        expect(triggerResponse2.ok()).toBeTruthy();

        const runData2 = (await triggerResponse2.json()) as { dag_run_id: string };
        await page.request.patch(
            `${baseUrl}/api/v2/dags/${testDagId}/dagRuns/${runData2.dag_run_id}`,
            {
                data: JSON.stringify({ state: "failed" }),
                headers: { "Content-Type": "application/json" },
            }
        );

        await context.close();
    });

    test.beforeEach(({ page }) => {
        dagCalendarPage = new DagCalendarPage(page);
    });

    test("verify calendar renders and displays runs", async () => {
        await dagCalendarPage.navigate(); // Should go to dags list
        await dagCalendarPage.navigateToCalendar(testDagId);

        await dagCalendarPage.verifyCalendarRender();

        const today = new Date().toISOString().split("T")[0];

        // We triggered runs for today, so we should see Mixed or at least one of them
        // If multiple runs on same day, logic might be mixed.
        // CalendarCell.tsx handles mixed state if it has both failed and success?
        // Wait, types.ts says run state can be mixed? No, DagRunState is single.
        // But CalendarCell props: backgroundColor can be object {actual, planned} or single string.
        // If we have success and failed, it likely shows 'failed' color in 'failed' view mode, or both?
        // checking CalendarTooltip: it lists all states.

        // Let's verify we can find the cell for today
        const cell = dagCalendarPage.page.locator(`[data-testid="calendar-cell"][data-date="${today}"]`);
        await expect(cell).toBeVisible();

        // Hover and check tooltip
        await cell.hover();
        const tooltip = dagCalendarPage.page.getByRole("tooltip");
        await expect(tooltip).toBeVisible();
        await expect(tooltip).toContainText(today);
        await expect(tooltip).toContainText("Success");
        await expect(tooltip).toContainText("Failed");
    });

    test("verify status filtering", async () => {
        await dagCalendarPage.navigateToCalendar(testDagId);

        // Click 'Failed runs' button if exists (from Calendar.tsx: setViewMode("failed"))
        await dagCalendarPage.page.getByRole("button", { name: "Failed Runs" }).click();

        const today = new Date().toISOString().split("T")[0];
        const cell = dagCalendarPage.page.locator(`[data-testid="calendar-cell"][data-date="${today}"]`);

        await cell.hover();
        const tooltip = dagCalendarPage.page.getByRole("tooltip");
        await expect(tooltip).toContainText("Failed");
        // Should NOT contain Success count if filtered? 
        // CalendarTooltip logic: viewMode === "failed" ? return key === "failed" ...
        // Yes, it filters out non-failed.
        await expect(tooltip).not.toContainText("Success");
    });
});
