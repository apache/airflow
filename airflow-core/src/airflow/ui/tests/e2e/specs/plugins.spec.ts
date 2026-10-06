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

test.describe("Plugins Page", () => {
  test.beforeEach(async ({ pluginsPage }) => {
    await pluginsPage.navigate();
  });

  test("verify plugins list displays each plugin with a name and source", async ({ pluginsPage }) => {
    await expect(pluginsPage.heading).toBeVisible();
    await expect(pluginsPage.table).toBeVisible();
    await expect(pluginsPage.rows).not.toHaveCount(0);

    const count = await pluginsPage.rows.count();

    await expect(pluginsPage.nameColumn).toHaveCount(count);
    await expect(pluginsPage.sourceColumn).toHaveCount(count);

    for (let i = 0; i < count; i++) {
      await expect(pluginsPage.nameColumn.nth(i)).not.toBeEmpty();
      await expect(pluginsPage.sourceColumn.nth(i)).not.toBeEmpty();
    }
  });
});
