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
import { test } from "tests/e2e/fixtures/task-instances-data";

test.describe("Task Instances Page", () => {
  test.setTimeout(60_000);

  // taskInstancesData is triggered once per worker via beforeEach.
  // eslint-disable-next-line @typescript-eslint/no-empty-function -- triggers worker-scoped data fixture
  test.beforeEach(async ({ taskInstancesData: _data }) => {});

  test("verify task instances table displays data", async ({ taskInstancesPage }) => {
    await taskInstancesPage.navigate();
    await taskInstancesPage.verifyTaskInstancesExist();
  });

  test("verify task details display correctly", async ({ taskInstancesPage }) => {
    await taskInstancesPage.navigate();
    await taskInstancesPage.verifyTaskDetailsDisplay();
  });

  test("verify filtering by failed state", async ({ taskInstancesData, taskInstancesPage }) => {
    await taskInstancesPage.navigate();
    await taskInstancesPage.verifyStateFiltering("Failed", taskInstancesData.dagId);
  });

  test("verify filtering by success state", async ({ taskInstancesData, taskInstancesPage }) => {
    await taskInstancesPage.navigate();
    await taskInstancesPage.verifyStateFiltering("Success", taskInstancesData.dagId);
  });
});
