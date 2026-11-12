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
import { ChakraProvider, defaultSystem } from "@chakra-ui/react";
import "@testing-library/jest-dom/vitest";
import { fireEvent, render, screen, within } from "@testing-library/react";
import { MemoryRouter, Outlet, Route, Routes, useLocation } from "react-router-dom";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { useContainerWidth } from "src/utils";

import { NavTabs, type NavTab } from "./NavTabs";

vi.mock("src/utils", async () => {
  const actual = await vi.importActual("src/utils");

  return { ...actual, useContainerWidth: vi.fn() };
});

// Every tab, and the "More" trigger, measures 100px, except a tab labelled "Wide".
const TAB_WIDTH = 100;
const WIDE_TAB_WIDTH = 300;

const TABS: Array<NavTab> = Array.from({ length: 9 }, (_, index) => ({
  label: `Tab ${index}`,
  value: `tab${index}`,
}));

const CurrentPath = () => {
  const { pathname, search } = useLocation();

  return <div data-testid="current-path">{`${pathname}${search}`}</div>;
};

const renderTabs = (initialPath: string, tabs: Array<NavTab> = TABS) =>
  render(
    <ChakraProvider value={defaultSystem}>
      <MemoryRouter initialEntries={[initialPath]}>
        <Routes>
          <Route
            element={
              <>
                <NavTabs tabs={tabs} />
                <Outlet />
              </>
            }
            path="/dag"
          >
            <Route element={<CurrentPath />} path=":tab" />
            <Route element={<CurrentPath />} path="plugin/:page/*" />
          </Route>
        </Routes>
      </MemoryRouter>
    </ChakraProvider>,
  );

const getRowLabels = () =>
  screen
    .getAllByRole("link")
    .filter((link) => link.closest("[role=menu]") === null)
    .map((link) => link.textContent);

describe("NavTabs", () => {
  beforeEach(() => {
    vi.spyOn(HTMLElement.prototype, "getBoundingClientRect").mockImplementation(function getRect(
      this: HTMLElement,
    ) {
      return { width: this.textContent === "Wide" ? WIDE_TAB_WIDTH : TAB_WIDTH } as DOMRect;
    });
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  it("renders every tab in the row when they all fit", () => {
    vi.mocked(useContainerWidth).mockReturnValue(TABS.length * TAB_WIDTH);

    renderTabs("/dag/tab0");

    expect(getRowLabels()).toEqual(TABS.map(({ label }) => label));
    expect(screen.queryByTestId("nav-tabs-more")).not.toBeInTheDocument();
  });

  it("keeps every tab in the row until the panel has been measured", () => {
    vi.mocked(useContainerWidth).mockReturnValue(0);

    renderTabs("/dag/tab0");

    expect(getRowLabels()).toEqual(TABS.map(({ label }) => label));
  });

  it("moves the tabs that do not fit to the More menu", async () => {
    vi.mocked(useContainerWidth).mockReturnValue(700);

    renderTabs("/dag/tab0");

    // 700px leaves room for the "More" trigger and six tabs.
    expect(getRowLabels()).toEqual(["Tab 0", "Tab 1", "Tab 2", "Tab 3", "Tab 4", "Tab 5"]);

    fireEvent.click(screen.getByTestId("nav-tabs-more"));
    const menu = await screen.findByRole("menu");

    expect(
      within(menu)
        .getAllByRole("menuitem")
        .map((item) => item.textContent),
    ).toEqual(["Tab 6", "Tab 7", "Tab 8"]);

    fireEvent.click(within(menu).getByText("Tab 7"));

    expect(screen.getByTestId("current-path")).toHaveTextContent("/dag/tab7");
  });

  it("moves every tab after the first one that does not fit to the menu", () => {
    vi.mocked(useContainerWidth).mockReturnValue(450);

    renderTabs("/dag/tab0", [
      { label: "Tab 0", value: "tab0" },
      { label: "Wide", value: "wide" },
      { label: "Tab 2", value: "tab2" },
      { label: "Tab 3", value: "tab3" },
    ]);

    // Tab 2 and Tab 3 would fit after Tab 0, but the row fills in order and stops at Wide.
    expect(getRowLabels()).toEqual(["Tab 0"]);
  });

  it("keeps a tab active through matchPaths in the row", () => {
    vi.mocked(useContainerWidth).mockReturnValue(700);

    renderTabs("/dag/logs", [...TABS.slice(0, 8), { label: "Tab 8", matchPaths: ["logs"], value: "tab8" }]);

    expect(getRowLabels()).toEqual(["Tab 0", "Tab 1", "Tab 2", "Tab 3", "Tab 4", "Tab 8"]);
  });

  it("keeps a plugin tab active on the plugin's sub-routes", () => {
    vi.mocked(useContainerWidth).mockReturnValue(700);

    renderTabs("/dag/plugin/my-view/details", [
      ...TABS.slice(0, 8),
      { label: "My View", value: "plugin/my-view" },
    ]);

    expect(getRowLabels()).toEqual(["Tab 0", "Tab 1", "Tab 2", "Tab 3", "Tab 4", "My View"]);
  });

  it("keeps the search of a tab opened from the menu", async () => {
    vi.mocked(useContainerWidth).mockReturnValue(700);

    renderTabs("/dag/tab0", [
      ...TABS.slice(0, 8),
      { label: "Tab 8", search: "?try_number=2", value: "tab8" },
    ]);

    fireEvent.click(screen.getByTestId("nav-tabs-more"));
    fireEvent.click(within(await screen.findByRole("menu")).getByText("Tab 8"));

    expect(screen.getByTestId("current-path")).toHaveTextContent("/dag/tab8?try_number=2");
  });

  it("keeps the active tab in the row when it would overflow", () => {
    vi.mocked(useContainerWidth).mockReturnValue(700);

    renderTabs("/dag/tab8");

    expect(getRowLabels()).toEqual(["Tab 0", "Tab 1", "Tab 2", "Tab 3", "Tab 4", "Tab 8"]);
    expect(within(screen.getByRole("link", { name: "Tab 8" })).getByText("Tab 8")).toHaveAttribute(
      "aria-current",
      "page",
    );
  });

  it("shows icons and an icon-only More trigger in a narrow panel", async () => {
    vi.mocked(useContainerWidth).mockReturnValue(400);

    renderTabs(
      "/dag/tab0",
      TABS.map((tab) => ({ ...tab, icon: <span>{`icon ${tab.value}`}</span> })),
    );

    expect(getRowLabels()).toEqual(["icon tab0", "icon tab1", "icon tab2"]);
    const more = screen.getByRole("button", { name: "moreTabs" });

    expect(more).not.toHaveTextContent("moreTabs");

    fireEvent.click(more);
    const menu = await screen.findByRole("menu");

    expect(within(menu).getByRole("menuitem", { name: "Tab 3" })).toBeInTheDocument();
  });
});
