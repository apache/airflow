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
import type { APIRequestContext } from "@playwright/test";

import { expect, test } from "tests/e2e/fixtures";
import { safeCleanupDagRun, waitForDagReady } from "tests/e2e/utils/api/dag-runs";
import { baseUrl, uniqueRunId } from "tests/e2e/utils/shared";

const SENSOR_DAG_ID = "example_time_delta_sensor_async";
const WATCHER_DAG_ID = "example_asset_with_watchers";

// The example Dags can make task and asset watcher triggers; nothing here makes callback or failing ones.
const KINDS = [
  { label: "Task", value: "task" },
  { label: "Asset Watcher", value: "asset_watcher" },
];

const countRunningTriggers = async (request: APIRequestContext, kind: string): Promise<number> => {
  const response = await request.get(`${baseUrl}/api/v2/triggers?kind=${kind}&status=running&limit=1`);

  return response.ok() ? ((await response.json()) as { total_entries: number }).total_entries : 0;
};

test.describe("Triggers page", () => {
  test.describe.configure({ mode: "serial" });
  test.setTimeout(120_000);

  let sensorRunId: string | undefined;

  test.beforeAll(async ({ authenticatedRequest }) => {
    test.setTimeout(300_000);
    const response = await authenticatedRequest.get(`${baseUrl}/api/v2/triggers?limit=1`);

    test.skip(response.status() === 404, "This Airflow has no GET /api/v2/triggers");

    // A watcher's trigger exists only while its Dag is unpaused; the global teardown pauses both Dags again.
    for (const dagId of [WATCHER_DAG_ID, SENSOR_DAG_ID]) {
      await waitForDagReady(authenticatedRequest, dagId);
      await authenticatedRequest.patch(`${baseUrl}/api/v2/dags/${dagId}`, { data: { is_paused: false } });
    }

    // The sensor waits for its run's data interval to end, so an hour-long interval keeps it deferred.
    const now = new Date();

    sensorRunId = uniqueRunId(SENSOR_DAG_ID);
    const created = await authenticatedRequest.post(`${baseUrl}/api/v2/dags/${SENSOR_DAG_ID}/dagRuns`, {
      data: {
        dag_run_id: sensorRunId,
        data_interval_end: new Date(now.getTime() + 3_600_000).toISOString(),
        data_interval_start: now.toISOString(),
        logical_date: now.toISOString(),
      },
    });

    expect(created.ok()).toBe(true);

    for (const kind of KINDS) {
      await expect
        .poll(() => countRunningTriggers(authenticatedRequest, kind.value), {
          intervals: [5000],
          timeout: 240_000,
        })
        .toBeGreaterThan(0);
    }
  });

  test.afterAll(async ({ authenticatedRequest }) => {
    if (sensorRunId !== undefined) {
      await safeCleanupDagRun(authenticatedRequest, SENSOR_DAG_ID, sensorRunId);
    }
  });

  test("lists triggers by kind", async ({ triggersPage }) => {
    for (const kind of KINDS) {
      await triggersPage.navigate(`?kind=${kind.value}`);
      // Retried: the table shows placeholder rows with empty cells while it loads.
      await expect(async () => {
        const kinds = await triggersPage.columnTexts("Kind");

        expect(kinds.length).toBeGreaterThan(0);
        expect(new Set(kinds)).toEqual(new Set([kind.label]));
      }).toPass({ timeout: 30_000 });
    }
  });

  test("filters to running watchers and opens one", async ({ page, triggersPage }) => {
    await triggersPage.navigate();
    await triggersPage.addFilter("Kind", "Asset Watcher");
    await triggersPage.addFilter("Status", "Running");

    await expect(page).toHaveURL(/kind=asset_watcher/u);
    await expect(page).toHaveURL(/status=running/u);
    await expect(async () => {
      expect(new Set(await triggersPage.columnTexts("Kind"))).toEqual(new Set(["Asset Watcher"]));
      expect(new Set(await triggersPage.columnTexts("Status"))).toEqual(new Set(["Running"]));
    }).toPass({ timeout: 30_000 });

    await triggersPage.tableRows.first().getByRole("link").first().click();
    await expect(page).toHaveURL(/\/triggers\/\d+$/u);
    await expect(page.getByText(/FileDeleteTrigger/u).first()).toBeVisible({ timeout: 30_000 });

    await page.getByTestId("trigger-breadcrumb").getByRole("link").click();
    await expect(page).toHaveURL(/\/triggers$/u);
  });
});
