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
import { test } from "tests/e2e/fixtures/xcom-data";

test.describe("XComs Page", () => {
  test.setTimeout(60_000);

  // xcomRunsData is triggered once per worker via beforeEach.
  // eslint-disable-next-line @typescript-eslint/no-empty-function -- triggers worker-scoped data fixture
  test.beforeEach(async ({ xcomRunsData: _data }) => {});

  test("verify XComs table renders", async ({ xcomsPage }) => {
    await xcomsPage.navigate();
    await expect(xcomsPage.xcomsTable).toBeVisible();
  });

  test("verify XComs table displays data", async ({ xcomsPage }) => {
    await xcomsPage.navigate();
    await xcomsPage.verifyXComsExist();
  });

  test("verify XCom details display correctly", async ({ xcomsPage }) => {
    await xcomsPage.navigate();
    await xcomsPage.verifyXComDetailsDisplay();
  });

  test("verify XCom values can be viewed", async ({ xcomsPage }) => {
    await xcomsPage.navigate();
    await xcomsPage.verifyXComValuesDisplayed();
  });

  test("verify expand/collapse functionality", async ({ xcomsPage }) => {
    await xcomsPage.verifyExpandCollapse();
  });

  test("verify filtering by key pattern", async ({ xcomRunsData, xcomsPage }) => {
    await xcomsPage.verifyKeyPatternFiltering(xcomRunsData.xcomKey);
  });

  test("verify filtering by Dag display name", async ({ xcomRunsData, xcomsPage }) => {
    await xcomsPage.verifyDagDisplayNameFiltering(xcomRunsData.dagId);
  });
});
