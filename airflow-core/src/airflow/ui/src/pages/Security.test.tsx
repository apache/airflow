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

import { BaseWrapper } from "src/utils/Wrapper";

import { Security } from "./Security";

const navigate = vi.hoisted(() => vi.fn());

vi.mock("react-router-dom", async (importOriginal) => ({
  ...(await importOriginal<typeof ReactRouterDom>()),
  useNavigate: () => navigate,
}));

vi.mock("openapi/queries", () => ({
  useAuthLinksServiceGetAuthMenus: () => ({
    data: { authorized_menu_items: [], extra_menu_items: [{ href: "/auth/users/list/", text: "Users" }] },
    isLoading: false,
  }),
}));

const renderAt = (entry: string) =>
  render(
    <MemoryRouter initialEntries={[entry]}>
      <Routes>
        <Route element={<Security />} path="security/:page/*" />
      </Routes>
    </MemoryRouter>,
    { wrapper: BaseWrapper },
  );

const setFrameLocation = (iframe: HTMLIFrameElement | null, pathname: string) =>
  Object.defineProperty(iframe, "contentWindow", {
    configurable: true,
    value: { location: { hash: "", href: `http://localhost${pathname}`, pathname, search: "" } },
  });

beforeEach(() => {
  navigate.mockClear();
});

describe("Security view URL sync", () => {
  it("loads the deep-linked inner auth page when the URL carries one", () => {
    const { container } = renderAt("/security/users/auth/users/edit/2");

    expect(container.querySelector("iframe")?.getAttribute("src")).toBe("/auth/users/edit/2");
  });

  it("mirrors in-iframe navigation into the address bar under the security route", () => {
    const { container } = renderAt("/security/users");
    const iframe = container.querySelector("iframe");

    setFrameLocation(iframe, "/auth/users/edit/2");
    navigate.mockClear();
    fireEvent.load(iframe as HTMLIFrameElement);

    expect(navigate).toHaveBeenCalledWith(
      { hash: "", pathname: "/security/users/auth/users/edit/2", search: "" },
      { replace: true },
    );
  });

  it("sends the user home when the framed page leaves the auth area", () => {
    const { container } = renderAt("/security/users");
    const iframe = container.querySelector("iframe");

    setFrameLocation(iframe, "/dags");
    navigate.mockClear();
    fireEvent.load(iframe as HTMLIFrameElement);

    expect(navigate).toHaveBeenCalledWith("/");
  });
});
