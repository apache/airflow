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
import "@testing-library/jest-dom/vitest";
import { fireEvent, render, screen } from "@testing-library/react";
import type * as ReactRouterDom from "react-router-dom";
import { MemoryRouter } from "react-router-dom";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import i18n from "src/i18n/config";
import { BaseWrapper } from "src/utils/Wrapper";

import commonLocale from "../../public/i18n/locales/en/common.json";
import { ErrorPage } from "./Error";

const { navigate, useRouteError } = vi.hoisted(() => ({
  navigate: vi.fn(),
  useRouteError: vi.fn(),
}));

vi.mock("react-router-dom", async (importOriginal) => ({
  ...(await importOriginal<typeof ReactRouterDom>()),
  useNavigate: () => navigate,
  useRouteError,
}));

const renderErrorPage = () =>
  render(
    <MemoryRouter>
      <ErrorPage />
    </MemoryRouter>,
    { wrapper: BaseWrapper },
  );

beforeEach(() => {
  navigate.mockClear();
  useRouteError.mockReset();
  i18n.addResourceBundle("en", "common", commonLocale, true, true);
});

afterEach(() => vi.restoreAllMocks());

describe("ErrorPage", () => {
  it("reloads the page after a route error", () => {
    const reload = vi.spyOn(globalThis.location, "reload").mockImplementation(() => undefined);

    useRouteError.mockReturnValue(new Error("Route module unavailable"));
    renderErrorPage();
    fireEvent.click(screen.getByRole("button", { name: "Reload page" }));

    expect(reload).toHaveBeenCalledOnce();
  });

  it("does not offer a reload for an unmatched route", () => {
    useRouteError.mockReturnValue(undefined);
    renderErrorPage();

    expect(screen.queryByRole("button", { name: "Reload page" })).not.toBeInTheDocument();
  });
});
