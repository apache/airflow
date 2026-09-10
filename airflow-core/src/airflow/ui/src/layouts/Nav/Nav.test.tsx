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
import type { PropsWithChildren } from "react";

import "@testing-library/jest-dom";
import { render, screen } from "@testing-library/react";
import i18n from "i18next";
import { initReactI18next } from "react-i18next";
import { MemoryRouter } from "react-router-dom";
import { beforeAll, beforeEach, describe, expect, it, vi } from "vitest";

import { BaseWrapper } from "src/utils/Wrapper";

import enCommon from "../../../public/i18n/locales/en/common.json";
import zhTWCommon from "../../../public/i18n/locales/zh-TW/common.json";
import { Nav } from "./Nav";

vi.mock("openapi/queries", () => ({
  useAuthLinksServiceGetAuthMenus: () => ({ data: undefined }),
  usePluginServiceGetPlugins: () => ({ data: undefined }),
  useVersionServiceGetVersion: () => ({ data: undefined }),
}));
vi.mock("src/context/timezone", () => ({ useTimezone: () => ({ selectedTimezone: "UTC" }) }));
vi.mock("src/utils/datetimeUtils", () => ({
  getTimezoneOffsetString: () => "+00:00",
  getTimezoneTooltipLabel: () => "UTC",
}));
vi.mock("src/queries/useConfig", () => ({ useConfig: () => undefined }));
vi.mock("src/components/Logo", () => ({ Logo: () => <div /> }));
vi.mock("./AdminButton", () => ({ AdminButton: () => <div /> }));
vi.mock("./BrowseButton", () => ({ BrowseButton: () => <div /> }));
vi.mock("./DocsButton", () => ({ DocsButton: () => <div /> }));
vi.mock("./SecurityButton", () => ({ SecurityButton: () => <div /> }));
vi.mock("./UserSettingsButton", () => ({ UserSettingsButton: () => <div /> }));
vi.mock("./PluginMenus", () => ({ PluginMenus: () => <div /> }));
vi.mock("./TimezoneModal", () => ({ default: () => <div /> }));

beforeAll(async () => {
  await i18n.use(initReactI18next).init({
    defaultNS: "common",
    fallbackLng: "en",
    interpolation: { escapeValue: false },
    lng: "en",
    resources: {
      en: { common: enCommon },
      "zh-TW": { common: zhTWCommon },
    },
  });
});

beforeEach(async () => {
  await i18n.changeLanguage("en");
});

const wrapperAt = (path: string) => {
  const wrapper = ({ children }: PropsWithChildren) => (
    <BaseWrapper>
      <MemoryRouter initialEntries={[path]}>{children}</MemoryRouter>
    </BaseWrapper>
  );

  return wrapper;
};

describe("Nav landmark", () => {
  it.each([
    { language: "en", name: "Navigation" },
    { language: "zh-TW", name: "導覽" },
  ])("exposes a translated navigation landmark in $language", async ({ language, name }) => {
    await i18n.changeLanguage(language);
    render(<Nav />, { wrapper: wrapperAt("/") });

    expect(screen.getByRole("navigation", { name })).toBeInTheDocument();
  });
});

describe("Nav dashboard button", () => {
  it("links to the dashboard at /home", () => {
    render(<Nav />, { wrapper: wrapperAt("/") });

    expect(screen.getByTestId("nav-dashboard-link")).toHaveAttribute("href", "/home");
  });

  it("is active on the dashboard route", () => {
    render(<Nav />, { wrapper: wrapperAt("/home") });

    expect(screen.getByTestId("nav-dashboard-link")).toHaveAttribute("aria-current", "page");
  });

  it("is not active on the landing page", () => {
    render(<Nav />, { wrapper: wrapperAt("/") });

    expect(screen.getByTestId("nav-dashboard-link")).not.toHaveAttribute("aria-current");
  });
});
