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

test.describe("Dag Bundles Page", () => {
  test.beforeEach(async ({ dagBundlesPage }) => {
    await dagBundlesPage.navigate();
  });

  test("verify dag bundles page heading", async ({ dagBundlesPage }) => {
    await expect(dagBundlesPage.heading).toBeVisible();
  });

  test("Verify Dag Bundles page is accessible via Browse menu", async ({ dagBundlesPage }) => {
    await dagBundlesPage.navigateFromBrowseMenu();

    await dagBundlesPage.waitForLoad();
    await expect(dagBundlesPage.heading).toBeVisible();
    expect(await dagBundlesPage.getRowCount()).toBeGreaterThan(0);
  });

  test("Verify the dag bundles list displays", async ({ dagBundlesPage }) => {
    await expect(dagBundlesPage.table).toBeVisible();
  });

  test("Verify the configured bundle is listed with a name and an active state", async ({
    dagBundlesPage,
  }) => {
    expect(await dagBundlesPage.getRowCount()).toBeGreaterThan(0);

    await expect(dagBundlesPage.nameCellAt(0)).not.toBeEmpty();
    // Spelled out both ways rather than left blank when healthy, so the cell is never empty.
    await expect(dagBundlesPage.activeCellAt(0)).toHaveText(/^(active|inactive)$/i);
  });

  test("Verify the version cell says what it knows", async ({ dagBundlesPage }) => {
    // The default `dags-folder` bundle does not support versioning, so this asserts the fallback
    // wording rather than a SHA. A versioning bundle would render the short hexsha instead, and
    // "Not refreshed yet" covers one that has never completed a refresh.
    await expect(dagBundlesPage.versionCellAt(0)).toHaveText(
      /^([\da-f]{7}|not versioned|not refreshed yet)$/i,
    );
  });

  test("Verify the last refreshed cell is populated", async ({ dagBundlesPage }) => {
    // Either a relative time or the "Never" fallback, but never blank.
    await expect(dagBundlesPage.lastRefreshedCellAt(0)).not.toBeEmpty();
  });

  test("Verify the import errors cell is populated", async ({ dagBundlesPage }) => {
    // A number when the caller may read import errors, "-" when they may not. Admin runs these,
    // so a count is expected -- but never a blank cell either way.
    await expect(dagBundlesPage.importErrorsCellAt(0)).toHaveText(/^(\d+|-)$/);
  });
});
