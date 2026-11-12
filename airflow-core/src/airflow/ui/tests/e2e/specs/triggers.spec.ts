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
import { expect, test } from "tests/e2e/fixtures";
import { waitForDagReady } from "tests/e2e/utils/api/dag-runs";
import { baseUrl } from "tests/e2e/utils/shared";

const WATCHER_DAG_ID = "example_asset_with_watchers";

test.describe("Triggers page", () => {
  test.setTimeout(120_000);

  test.beforeAll(async ({ authenticatedRequest }) => {
    test.setTimeout(300_000);
    const response = await authenticatedRequest.get(`${baseUrl}/api/v2/triggers?limit=1`);

    test.skip(response.status() === 404, "This Airflow has no GET /api/v2/triggers");

    // A watcher's trigger exists only while its Dag is unpaused; the global teardown pauses it again.
    await waitForDagReady(authenticatedRequest, WATCHER_DAG_ID);
    await authenticatedRequest.patch(`${baseUrl}/api/v2/dags/${WATCHER_DAG_ID}`, {
      data: { is_paused: false },
    });

    await expect
      .poll(
        async () => {
          const running = await authenticatedRequest.get(
            `${baseUrl}/api/v2/triggers?kind=asset_watcher&status=running&limit=1`,
          );

          return running.ok() ? ((await running.json()) as { total_entries: number }).total_entries : 0;
        },
        { intervals: [5000], timeout: 240_000 },
      )
      .toBeGreaterThan(0);
  });

  test("filters to running watchers and opens one", async ({ page, triggersPage }) => {
    await triggersPage.navigate();
    await triggersPage.addFilter("Kind", "Asset Watcher");
    await triggersPage.addFilter("Status", "Running");

    await expect(page).toHaveURL(/kind=asset_watcher/u);
    await expect(page).toHaveURL(/status=running/u);
    // Retried: the table shows placeholder rows with empty cells while it loads.
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
