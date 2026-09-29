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
import "@testing-library/jest-dom";
import { render, screen } from "@testing-library/react";
import { beforeEach, describe, expect, it, vi } from "vitest";

import { Wrapper } from "src/utils/Wrapper";

import { BackendsOrderCard } from "./BackendsOrderCard";

const mocks = vi.hoisted(() => ({
  useAuthLinksServiceGetAuthMenus: vi.fn(),
  useConfigServiceGetBackendsOrderValue: vi.fn(),
}));

vi.mock("openapi/queries", () => ({
  useAuthLinksServiceGetAuthMenus: mocks.useAuthLinksServiceGetAuthMenus,
  useConfigServiceGetBackendsOrderValue: mocks.useConfigServiceGetBackendsOrderValue,
}));

vi.mock("react-i18next", () => ({
  useTranslation: (namespace: string) => ({
    i18n: { dir: () => "ltr", language: "en" },
    // eslint-disable-next-line id-length
    t: (key: string) => `${namespace}:${key}`,
  }),
}));

const renderCard = (authorizedMenuItems: Array<string> | undefined) => {
  mocks.useAuthLinksServiceGetAuthMenus.mockReturnValue({
    data: authorizedMenuItems === undefined ? undefined : { authorized_menu_items: authorizedMenuItems },
  });
  mocks.useConfigServiceGetBackendsOrderValue.mockReturnValue({
    data: undefined,
    error: undefined,
    isLoading: false,
  });

  return render(<BackendsOrderCard />, { wrapper: Wrapper });
};

describe("BackendsOrderCard", () => {
  beforeEach(() => {
    mocks.useAuthLinksServiceGetAuthMenus.mockReset();
    mocks.useConfigServiceGetBackendsOrderValue.mockReset();
  });

  it("is shown to users with access to the Config page", () => {
    renderCard(["Variables", "Config"]);

    expect(screen.getByText("admin:variables.backendsOrder")).toBeInTheDocument();
  });

  it.each([
    ["without access to the Config page", ["Variables"]],
    ["while the authorized menus are loading", undefined],
  ])("is hidden %s", (_description, authorizedMenuItems) => {
    renderCard(authorizedMenuItems);

    expect(screen.queryByText("admin:variables.backendsOrder")).not.toBeInTheDocument();
  });
});
