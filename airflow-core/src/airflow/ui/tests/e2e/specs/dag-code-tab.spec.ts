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
import { test, expect } from "@playwright/test";
import { DagsPage } from "tests/e2e/pages/DagsPage";
import { testConfig } from "playwright.config";

test.describe("Dag Code Tab", () => {
  let dagsPage: DagsPage;
  const testDagId = testConfig.testDag.id;

  test.beforeEach(async ({ page }) => {
    dagsPage = new DagsPage(page);
  });

  test("verify Dag code displays", async () => {
    await dagsPage.openCodeTab(testDagId);

    const codeText = await dagsPage.getDagCodeText();
    expect(codeText.trim().length).toBeGreaterThan(0);
  });

  test("verify syntax highlighting is applied", async () => {
    await dagsPage.openCodeTab(testDagId);

    const styledSpans = dagsPage
      .getMonacoEditor()
      .locator(".view-line span");

    const count = await styledSpans.count();
    expect(count).toBeGreaterThan(5);
  });

  test("verify line numbers are displayed", async () => {
    await dagsPage.openCodeTab(testDagId);

    const lineNumbers = dagsPage
      .getMonacoEditor()
      .locator(".margin-view-overlays");

    await expect(lineNumbers).toBeVisible();
  });

  test("verify code is scrollable for long files", async () => {
    await dagsPage.openCodeTab(testDagId);

    const scrollContainer = dagsPage
      .getMonacoEditor()
      .locator(".monaco-scrollable-element");

    await expect(scrollContainer).toBeVisible();
  });

  test("verify wrap works", async () => {
    await dagsPage.openCodeTab(testDagId);

    const wrappedLines = dagsPage
      .getMonacoEditor()
      .locator(".view-line")
      .filter({ has: dagsPage.getMonacoEditor().locator("span") });

    const count = await wrappedLines.count();
    expect(count).toBeGreaterThan(0);
  });
});
