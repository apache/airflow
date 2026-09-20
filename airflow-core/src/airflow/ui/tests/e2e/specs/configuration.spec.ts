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

test.describe("Configuration Page", () => {
  test.beforeEach(async ({ configurationPage }) => {
    await configurationPage.navigate();
  });

  test("verify configuration displays", async ({ configurationPage }) => {
    await expect(configurationPage.heading).toBeVisible();
    await expect(configurationPage.table).toBeVisible();

    await expect(configurationPage.rows).not.toHaveCount(0);

    const firstRow = configurationPage.rows.nth(0);

    await expect(firstRow.getByTestId("table-cell-section")).not.toBeEmpty();
    await expect(firstRow.getByTestId("table-cell-key")).not.toBeEmpty();

    // Many options legitimately have an empty value, so check a row that always has one.
    const dagsFolderRow = configurationPage.getRowByKey("dags_folder");

    await expect(dagsFolderRow).toHaveCount(1);
    await expect(dagsFolderRow.getByTestId("table-cell-section")).toHaveText("core");
    await expect(dagsFolderRow.getByTestId("table-cell-value")).not.toBeEmpty();
  });
});
