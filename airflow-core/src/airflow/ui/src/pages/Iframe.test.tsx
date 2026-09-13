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
import { fireEvent, render } from "@testing-library/react";
import type * as ReactRouterDom from "react-router-dom";
import { MemoryRouter, Route, Routes } from "react-router-dom";
import { beforeEach, describe, expect, it, vi } from "vitest";

import type { ExternalViewResponse } from "openapi/requests/types.gen";

import { BaseWrapper } from "src/utils/Wrapper";

import { Iframe } from "./Iframe";

const navigate = vi.hoisted(() => vi.fn());

vi.mock("react-router-dom", async (importOriginal) => ({
  ...(await importOriginal<typeof ReactRouterDom>()),
  useNavigate: () => navigate,
}));

vi.mock("openapi/queries", () => ({
  useAssetServiceGetAsset: () => ({ data: undefined }),
}));

const navView = {
  destination: "nav",
  href: "/pluginsv2/",
  name: "Legacy FAB views",
  url_route: "legacy-fab-views",
} as ExternalViewResponse;

const renderAt = (entry: string) =>
  render(
    <MemoryRouter initialEntries={[entry]}>
      <Routes>
        <Route element={<Iframe externalView={navView} />} path="plugin/:page/*" />
      </Routes>
    </MemoryRouter>,
    { wrapper: BaseWrapper },
  );

beforeEach(() => {
  navigate.mockClear();
});

describe("Iframe URL sync", () => {
  it("loads the deep-linked inner path when the URL carries one", () => {
    const { container } = renderAt("/plugin/legacy-fab-views/pluginsv2/emptypluginview/");

    expect(container.querySelector("iframe")?.getAttribute("src")).toBe("/pluginsv2/emptypluginview/");
  });

  it("falls back to the view's entry href when the URL has no inner path", () => {
    const { container } = renderAt("/plugin/legacy-fab-views");

    expect(container.querySelector("iframe")?.getAttribute("src")).toBe("/pluginsv2/");
  });

  it("ignores a cross-origin deep link and falls back to the entry href", () => {
    // A protocol-relative segment crafted into the URL must not point the iframe off-origin.
    const { container } = renderAt("/plugin/legacy-fab-views//evil.com/x");

    expect(container.querySelector("iframe")?.getAttribute("src")).toBe("/pluginsv2/");
  });

  it("mirrors the iframe's location into the address bar when it navigates internally", () => {
    const { container } = renderAt("/plugin/legacy-fab-views");
    const iframe = container.querySelector("iframe");

    // Stand in for the iframe having navigated to an inner page (jsdom does not load real content).
    Object.defineProperty(iframe, "contentWindow", {
      configurable: true,
      value: { location: { hash: "", pathname: "/pluginsv2/emptypluginview/", search: "?q=1" } },
    });
    navigate.mockClear();
    fireEvent.load(iframe as HTMLIFrameElement);

    expect(navigate).toHaveBeenCalledWith(
      { hash: "", pathname: "/plugin/legacy-fab-views/pluginsv2/emptypluginview/", search: "?q=1" },
      { replace: true },
    );
  });
});
