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

  test.describe("Dag Code Tab", () => {
    let dagsPage: DagsPage;
    const testDagId = "example_simplest_dag";

    test.beforeEach(async ({ page }) => {
      dagsPage = new DagsPage(page);
    });

    test("should display Dag code", async () => {
      await dagsPage.navigateToDagDetail(testDagId);
    
      await dagsPage.openCodeTab();
    
      const codeText = await dagsPage.getDagCodeText();

      expect(codeText).not.toBeNull();
      expect(codeText?.trim().length).toBeGreaterThan(0);
    });  
  });