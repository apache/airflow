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

    test("verify DAG code displays", async () => {
      await dagsPage.openCodeTab(testDagId);
    
      const codeText = await dagsPage.getDagCodeText();

      expect(codeText).not.toBeNull();
      expect(codeText?.trim().length).toBeGreaterThan(0);
    });  

    test("verify syntax highlighting is applied", async ({ page }) => {
      await dagsPage.openCodeTab(testDagId);
      const highlightedTokens = page
      .getByTestId("dag-code-content")
      .locator(".token, .hljs");    
      await expect(highlightedTokens.first()).toBeVisible();
    });

    test("verify line numbers are displayed", async ({ page }) => {
      await dagsPage.openCodeTab(testDagId);
      await expect(page
        .getByTestId("dag-code-content")
        .locator("[class*=line]")
        .first()).toBeVisible();
    });

    test("verify code is scrollable for long files", async ({ page }) => {
      await dagsPage.openCodeTab(testDagId);
      const codeContainer = page.getByTestId("dag-code-content");
      await expect(codeContainer).toHaveCSS("overflow", /auto|scroll/);
    });

    test("verify code wrapping works", async ({ page }) => {
      await dagsPage.openCodeTab(testDagId);
      const codeContainer = page.getByTestId("dag-code-content");
      await expect(codeContainer).toHaveCSS("white-space", /pre-wrap|break-spaces/);
    }); 
  });