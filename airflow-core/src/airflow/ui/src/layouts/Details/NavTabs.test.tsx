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

// Every tab, and the "More" trigger, measures 100px.
const TAB_WIDTH = 100;

const TABS: Array<NavTab> = Array.from({ length: 9 }, (_, index) => ({
  label: `Tab ${index}`,
  value: `tab${index}`,
}));

const CurrentPath = () => <div data-testid="current-path">{useLocation().pathname}</div>;

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
    vi.spyOn(HTMLElement.prototype, "offsetWidth", "get").mockReturnValue(TAB_WIDTH);
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

    expect(within(menu).getByRole("menuitem", { name: "icon tab3 Tab 3" })).toBeInTheDocument();
  });
});
