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
import { loadRouteModule } from "src/utils/routeModule";

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
  globalThis.sessionStorage.clear();
  i18n.addResourceBundle("en", "common", commonLocale, true, true);
});

afterEach(() => {
  vi.restoreAllMocks();
  globalThis.sessionStorage.clear();
});

describe("ErrorPage", () => {
  it("offers a manual reload after a route error", () => {
    const reload = vi.spyOn(globalThis.location, "reload").mockImplementation(() => undefined);

    useRouteError.mockReturnValue(new Error("Route module unavailable"));
    renderErrorPage();

    expect(reload).not.toHaveBeenCalled();

    fireEvent.click(screen.getByRole("button", { name: "Reload page" }));

    expect(reload).toHaveBeenCalledOnce();
  });

  it("reloads the committed route only once after route module loading fails", async () => {
    const error = new Error("Route module unavailable");
    const reload = vi.spyOn(globalThis.location, "reload").mockImplementation(() => undefined);

    useRouteError.mockReturnValue(error);
    await expect(loadRouteModule(() => Promise.reject(error))).rejects.toBe(error);
    const firstRender = renderErrorPage();

    expect(reload).toHaveBeenCalledOnce();

    firstRender.unmount();
    await expect(loadRouteModule(() => Promise.reject(error))).rejects.toBe(error);
    renderErrorPage();

    expect(reload).toHaveBeenCalledOnce();
  });

  it("does not offer a reload for an unmatched route", () => {
    useRouteError.mockReturnValue(undefined);
    renderErrorPage();

    expect(screen.queryByRole("button", { name: "Reload page" })).not.toBeInTheDocument();
  });
});
