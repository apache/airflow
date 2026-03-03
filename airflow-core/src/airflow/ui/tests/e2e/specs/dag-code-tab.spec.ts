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
import { test } from "@playwright/test";
import { testConfig } from "playwright.config";

import { DagCodePage } from "../pages/DagCodePage";

test.describe("DAG Code Tab", () => {
  test.setTimeout(60_000);
  const dagId = testConfig.testDag.id;
  let codePage: DagCodePage;

  test.beforeEach(async ({ page }) => {
    codePage = new DagCodePage(page);
    await codePage.navigateToCodeTab(dagId);
  });

  test("Verify DAG source code is displayed", async () => {
    await codePage.verifySourceCodeDisplayed();
  });

  test("Verify syntax highlighting is applied", async () => {
    await codePage.verifySyntaxHighlighting();
  });

  test("Verify code is scrollable for long files", async () => {
    await codePage.verifyCodeIsScrollable();
  });

  test("Verify line numbers are displayed", async () => {
    await codePage.verifyLineNumbersDisplayed();
  });
});
