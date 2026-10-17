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
import { expect } from "tests/e2e/fixtures";
import { test } from "tests/e2e/fixtures/task-tries-data";
import { waitForDagRunStatus } from "tests/e2e/utils/api/dag-runs";

test.setTimeout(180_000);

test("selects the failed and successful retry logs without mixing tries", async ({ page, tryRun }) => {
  const { dagId, runId } = tryRun;

  await page.goto(`/dags/${dagId}/runs/${runId}/tasks/run_job/logs`);
  const logs = page.getByTestId("virtualized-list");

  await expect(logs).toContainText("Try 2: reattaching to existing job:", { timeout: 30_000 });
  await expect(logs).not.toContainText("Simulated failure after submitting");
  await page.getByTestId("log-attempt-select-button-1").click();
  await expect(page).toHaveURL(/try_number=1/u);
  await expect(logs).toContainText("Simulated failure after submitting", { timeout: 30_000 });
  await expect(logs).not.toContainText("Try 2: reattaching to existing job:");

  await page.reload();
  await expect(logs).toContainText("Simulated failure after submitting", { timeout: 30_000 });
  await page.getByTestId("log-attempt-select-button-2").click();
  await expect(page).not.toHaveURL(/try_number=/u);
  await expect(logs).toContainText("Try 2: reattaching to existing job:", { timeout: 30_000 });
});

test.describe("Mapped task tries", () => {
  test.use({ tryDagId: "example_dynamic_task_mapping" });

  test("preserves historical tries on detail tabs without filtering mapped siblings", async ({
    authenticatedRequest,
    page,
    tryRun,
  }) => {
    const { dagId, runId } = tryRun;
    const taskPath = `/dags/${dagId}/runs/${runId}/tasks/add_one/mapped/2`;
    const clear = async (indexes: Array<number>, resetDagRuns: boolean) => {
      const response = await authenticatedRequest.post(`/api/v2/dags/${dagId}/clearTaskInstances`, {
        data: {
          dag_run_id: runId,
          dry_run: false,
          only_failed: false,
          reset_dag_runs: resetDagRuns,
          task_ids: indexes.map((index) => ["add_one", index]),
        },
      });

      await expect(response).toBeOK();
    };

    // Give the mapped siblings different current tries: index 0 stays on try 1, indexes 1 and 2 reach try 2.
    await clear([1, 2], true);
    await waitForDagRunStatus(authenticatedRequest, { dagId, expectedState: "success", runId });
    // Keep the Dag run finished so index 2's newly allocated try 3 cannot start during the assertions.
    await clear([2], false);

    // The pending try has no logs; the previous successful try must still be readable.
    await page.goto(`${taskPath}/logs`);
    await expect(page.getByText("This try hasn't started yet.", { exact: true })).toBeVisible();
    await expect(page.getByTestId("log-attempt-select-button-3")).toBeVisible();
    await expect(page.getByTestId("virtualized-list")).not.toBeVisible();
    await page.getByTestId("log-attempt-select-button-2").click();
    await expect(page).toHaveURL(/try_number=2/u);
    await expect(page.getByTestId("virtualized-list")).toContainText("Done. Returned value was: 4", {
      timeout: 30_000,
    });

    // Details and Logs describe the same try, so navigation and reload must preserve try 2.
    await page.getByRole("link", { exact: true, name: "Details" }).click();
    await expect(page).toHaveURL(/\/details\?try_number=2$/u);
    await expect(
      page.getByRole("row").filter({ has: page.getByRole("cell", { exact: true, name: "State" }) }),
    ).toContainText("success");
    await page.reload();
    await expect(page).toHaveURL(/\/details\?try_number=2$/u);
    await expect(
      page.getByRole("row").filter({ has: page.getByRole("cell", { exact: true, name: "Try Number" }) }),
    ).toContainText("2");
    await page.getByRole("link", { exact: true, name: "Logs" }).click();
    await expect(page).toHaveURL(/try_number=2/u);
    await expect(page.getByTestId("virtualized-list")).toContainText("Done. Returned value was: 4");

    // The collection must show every mapped sibling, regardless of the try selected for index 2.
    await page.getByRole("link", { name: /^Task Instances \[/u }).click();
    await expect(page).not.toHaveURL(/try_number=/u);
    const table = page.getByRole("table");

    await expect(table.getByRole("row")).toHaveCount(4);
    for (const index of [0, 1, 2]) {
      await expect(table.locator(`a[href*="/mapped/${index}"]`).first()).toBeVisible();
    }
  });
});
