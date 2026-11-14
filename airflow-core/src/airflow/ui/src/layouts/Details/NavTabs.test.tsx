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

import { usePersistentSearchParamKeys } from "src/constants/persistentSearchParams";
import { useContainerWidth } from "src/utils";
import { BaseWrapper } from "src/utils/Wrapper";

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
      return {
        width: this.textContent === "Wide" ? WIDE_TAB_WIDTH : TAB_WIDTH,
      } as DOMRect;
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
    expect(screen.getByRole("link", { name: "Tab 8" }).firstElementChild).toHaveAttribute(
      "aria-current",
      "page",
    );
  });

  it("renders each label's text once while carrying it for the bold-width copy", () => {
    vi.mocked(useContainerWidth).mockReturnValue(TABS.length * TAB_WIDTH);

    renderTabs("/dag/tab0");

    const link = screen.getByRole("link", { name: "Tab 1" });

    expect(within(link).getByText("Tab 1")).toHaveAttribute("data-label", "Tab 1");
    expect(link).toHaveTextContent(/^Tab 1$/u);
  });

  it("shows a tab's icon beside its label when showIconWithLabel is set", () => {
    vi.mocked(useContainerWidth).mockReturnValue(TABS.length * TAB_WIDTH);

    renderTabs("/dag/tab0", [
      {
        icon: <img alt="Warehouse" src="/icon.svg" />,
        label: "Warehouse",
        showIconWithLabel: true,
        value: "plugin/warehouse",
      },
    ]);

    const link = screen.getByRole("link", { name: "Warehouse" });

    expect(link).toHaveTextContent("Warehouse");
    expect(within(link).getByRole("img", { hidden: true })).toBeInTheDocument();
    expect(within(link).queryByRole("img")).not.toBeInTheDocument();
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

const DETAILS_TABS: Array<NavTab> = [
  { label: "Details", value: "details" },
  { label: "Logs", search: "try_number=2", value: "logs" },
];

const DetailsTabs = () => <NavTabs persistentKeys={usePersistentSearchParamKeys()} tabs={DETAILS_TABS} />;

const renderDetailsTabs = (search: string, carryFilters = true) => {
  render(
    <BaseWrapper>
      <MemoryRouter initialEntries={[`/dags/my_dag/runs/my_run${search}`]}>
        {carryFilters ? <DetailsTabs /> : <NavTabs tabs={DETAILS_TABS} />}
      </MemoryRouter>
    </BaseWrapper>,
  );
};

const linkSearch = (name: string) => {
  const href = screen.getByRole("link", { name }).getAttribute("href") ?? "";

  return new URL(href, "http://localhost").searchParams;
};

describe("NavTabs persistent filters", () => {
  beforeEach(() => {
    vi.mocked(useContainerWidth).mockReturnValue(DETAILS_TABS.length * TAB_WIDTH);
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  it("carries the grid filters onto every tab", () => {
    renderDetailsTabs("?state=failed&run_type=manual&run_after_gte=2026-01-01");

    expect(Object.fromEntries(linkSearch("Details"))).toStrictEqual({
      run_after_gte: "2026-01-01",
      run_type: "manual",
      state: "failed",
    });
  });

  it("carries the gantt date ranges by the start and end keys they write", () => {
    renderDetailsTabs("?start_date_gte=2026-01-01&end_date_lte=2026-02-01&start_date_range=ignored");

    expect(Object.fromEntries(linkSearch("Details"))).toStrictEqual({
      end_date_lte: "2026-02-01",
      start_date_gte: "2026-01-01",
    });
  });

  it("carries the graph and task stream filters, mode included", () => {
    renderDetailsTabs(
      "?graph-operator=BashOperator&root=extract&upstream=true&downstream=true&depth=2&mode=traverse",
    );

    expect(Object.fromEntries(linkSearch("Details"))).toStrictEqual({
      depth: "2",
      downstream: "true",
      "graph-operator": "BashOperator",
      mode: "traverse",
      root: "extract",
      upstream: "true",
    });
  });

  it("carries a filter that repeats, rather than only its first value", () => {
    renderDetailsTabs("?log_level=error&log_level=warning");

    expect(linkSearch("Details").getAll("log_level")).toStrictEqual(["error", "warning"]);
  });

  it("leaves one tab's paging and sorting behind", () => {
    renderDetailsTabs("?state=failed&offset=20&limit=50&sort=-run_after");

    expect(Object.fromEntries(linkSearch("Details"))).toStrictEqual({
      state: "failed",
    });
  });

  it("lets the tab decide the parameters it names itself", () => {
    renderDetailsTabs("?state=failed&try_number=7");

    expect(Object.fromEntries(linkSearch("Logs"))).toStrictEqual({
      state: "failed",
      try_number: "2",
    });
  });

  it("carries nothing unless the layout passes the keys", () => {
    renderDetailsTabs("?state=failed&log_level=error", false);

    expect(Object.fromEntries(linkSearch("Details"))).toStrictEqual({});
    expect(Object.fromEntries(linkSearch("Logs"))).toStrictEqual({
      try_number: "2",
    });
  });

  it("carries the filters onto a tab moved to the More menu", async () => {
    vi.spyOn(HTMLElement.prototype, "getBoundingClientRect").mockReturnValue({
      width: TAB_WIDTH,
    } as DOMRect);
    vi.mocked(useContainerWidth).mockReturnValue(TAB_WIDTH * 1.5);

    renderDetailsTabs("?state=failed");

    fireEvent.click(screen.getByTestId("nav-tabs-more"));
    const logs = within(await screen.findByRole("menu")).getByRole("menuitem", {
      name: "Logs",
    });

    expect(
      Object.fromEntries(new URL(logs.getAttribute("href") ?? "", "http://localhost").searchParams),
    ).toStrictEqual({
      state: "failed",
      try_number: "2",
    });
  });
});
