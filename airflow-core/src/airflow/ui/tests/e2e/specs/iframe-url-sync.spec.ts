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
import type { Page } from "@playwright/test";

import { expect, test } from "tests/e2e/fixtures";

const page = (heading: string, links: Array<{ href: string; id: string }> = []) =>
  `<html><body><h1>${heading}</h1>${links.map((lnk) => `<a href="${lnk.href}" id="${lnk.id}">${lnk.id}</a>`).join("")}</body></html>`;

// Stub the framed same-origin app with real (200) pages, so the sync is exercised regardless of the
// auth manager / plugins the backend happens to run.
const stubFramedApp = async (browserPage: Page, pages: Record<string, string>) => {
  await browserPage.route(
    (url) => Object.keys(pages).includes(url.pathname),
    (route) =>
      route.fulfill({ body: pages[new URL(route.request().url()).pathname], contentType: "text/html" }),
  );
};

test.describe("Iframe view URL sync (#55815)", () => {
  test("security views mirror in-iframe navigation into the address bar and deep-link", async ({
    page: browserPage,
  }) => {
    await stubFramedApp(browserPage, {
      "/auth/users/edit/2": page("Edit user 2"),
      "/auth/users/list/": page("List Users", [{ href: "/auth/users/edit/2", id: "edit" }]),
    });

    await browserPage.goto("/security/users");
    const frame = browserPage.frameLocator("iframe");

    await expect(frame.locator("h1")).toHaveText("List Users");
    await expect(browserPage).toHaveURL(/\/security\/users\/auth\/users\/list\/$/u);

    await frame.locator("#edit").click();
    await expect(frame.locator("h1")).toHaveText("Edit user 2");
    await expect(browserPage).toHaveURL(/\/security\/users\/auth\/users\/edit\/2$/u);

    await browserPage.evaluate(() => window.history.back());
    await expect(frame.locator("h1")).toHaveText("List Users");
    await expect(browserPage).toHaveURL(/\/security\/users\/auth\/users\/list\/$/u);

    // Deep-link straight to the edit page.
    await browserPage.goto("/security/users/auth/users/edit/2");
    await expect(browserPage.frameLocator("iframe").locator("h1")).toHaveText("Edit user 2");
    await expect(browserPage).toHaveURL(/\/security\/users\/auth\/users\/edit\/2$/u);
  });

  test("legacy plugin views mirror in-iframe navigation into the address bar and deep-link", async ({
    page: browserPage,
  }) => {
    await stubFramedApp(browserPage, {
      "/pluginsv2/": page("Home", [{ href: "/pluginsv2/some/inner/page", id: "inner" }]),
      "/pluginsv2/some/inner/page": page("Inner page"),
    });

    await browserPage.goto("/plugin/legacy-fab-views");
    const frame = browserPage.frameLocator("iframe");

    await expect(frame.locator("h1")).toHaveText("Home");
    await expect(browserPage).toHaveURL(/\/plugin\/legacy-fab-views\/pluginsv2\/$/u);

    await frame.locator("#inner").click();
    await expect(frame.locator("h1")).toHaveText("Inner page");
    await expect(browserPage).toHaveURL(/\/plugin\/legacy-fab-views\/pluginsv2\/some\/inner\/page$/u);

    await browserPage.goto("/plugin/legacy-fab-views/pluginsv2/some/inner/page");
    await expect(browserPage.frameLocator("iframe").locator("h1")).toHaveText("Inner page");
  });
});
