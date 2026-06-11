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
import { testConfig } from "playwright.config";
import { test } from "tests/e2e/fixtures";

test.describe("Dag Code Tab", () => {
  test.setTimeout(60_000);
  const dagId = testConfig.testDag.id;

  test.beforeEach(async ({ dagCodePage }) => {
    await dagCodePage.navigateToCodeTab(dagId);
  });

  test("Verify Dag source code is displayed", async ({ dagCodePage }) => {
    await dagCodePage.verifySourceCodeDisplayed();
  });

  test("Verify syntax highlighting is applied", async ({ dagCodePage }) => {
    await dagCodePage.verifySyntaxHighlighting();
  });

  test("Verify code is scrollable for long files", async ({ dagCodePage }) => {
    await dagCodePage.verifyCodeIsScrollable();
  });

  test("Verify line numbers are displayed", async ({ dagCodePage }) => {
    await dagCodePage.verifyLineNumbersDisplayed();
  });
});
