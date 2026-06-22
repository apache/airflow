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
import { fireEvent, render, screen } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import type { UIAlert } from "openapi/requests/types.gen";
import { COLLAPSED_UI_ALERTS_KEY } from "src/constants/localStorage";
import { Wrapper } from "src/utils/Wrapper";

import { Dashboard } from "./Dashboard";

const mockConfig: Record<string, unknown> = {};

vi.mock("src/queries/useConfig", () => ({
  useConfig: (key: string) => mockConfig[key],
}));

vi.mock("openapi/queries", () => ({
  usePluginServiceGetPlugins: () => ({ data: { plugins: [] } }),
}));

vi.mock("react-i18next", () => ({
  useTranslation: () => ({
    // eslint-disable-next-line id-length
    t: (key: string, options?: { count?: number }) =>
      key === "alerts.showMoreAlerts"
        ? `+${options?.count} more ${options?.count === 1 ? "alert" : "alerts"}`
        : key,
  }),
}));

vi.mock("./AlertContent", () => ({
  AlertContent: ({ alert }: { readonly alert: UIAlert }) => <div>{alert.text}</div>,
}));

// Ark UI's accordion machine does not respond to synthetic clicks under happy-dom, so this
// stand-in wires the controlled value/onValueChange contract manually.
vi.mock("src/components/ui", async () => {
  const { createContext, useContext } = await import("react");

  type AccordionContextValue = {
    onValueChange: (details: { value: Array<string> }) => void;
    value: Array<string>;
  };
  const AccordionContext = createContext<AccordionContextValue>({
    onValueChange: () => undefined,
    value: [],
  });

  type RootProps = { readonly children: React.ReactNode } & AccordionContextValue;
  const passthrough = ({ children }: { readonly children: React.ReactNode }) => <div>{children}</div>;

  return {
    Accordion: {
      Item: passthrough,
      ItemContent: passthrough,
      ItemTrigger: ({ children }: { readonly children: React.ReactNode }) => {
        const { onValueChange, value } = useContext(AccordionContext);
        const isOpen = value.includes("ui_alerts");

        return (
          <button
            data-part="item-trigger"
            data-state={isOpen ? "open" : "closed"}
            onClick={() => onValueChange({ value: isOpen ? [] : ["ui_alerts"] })}
            type="button"
          >
            {children}
          </button>
        );
      },
      Root: ({ children, onValueChange, value }: RootProps) => (
        <AccordionContext.Provider value={{ onValueChange, value }}>{children}</AccordionContext.Provider>
      ),
    },
  };
});

vi.mock("./Deadlines", () => ({ DashboardDeadlines: () => null }));
vi.mock("./FavoriteDags", () => ({ FavoriteDags: () => null }));
vi.mock("./Health", () => ({ Health: () => null }));
vi.mock("./HistoricalMetrics", () => ({ HistoricalMetrics: () => null }));
vi.mock("./PoolSummary", () => ({ PoolSummary: () => null }));
vi.mock("./Stats", () => ({ Stats: () => null }));
vi.mock("src/components/TimeRangeSelector", () => ({ default: () => null }));
vi.mock("../ReactPlugin", () => ({ ReactPlugin: () => null }));

const setAlerts = (alerts: Array<UIAlert>) => {
  mockConfig.dashboard_alert = alerts;
  mockConfig.instance_name = "Airflow";
};

const accordionState = (container: HTMLElement) =>
  container.querySelector<HTMLElement>('[data-part="item-trigger"]')?.dataset.state;

describe("Dashboard alerts accordion", () => {
  beforeEach(() => {
    globalThis.localStorage.clear();
  });

  afterEach(() => {
    globalThis.localStorage.clear();
  });

  it("does not write the collapsed key when no alerts are configured", () => {
    setAlerts([]);

    render(<Dashboard />, { wrapper: Wrapper });

    expect(globalThis.localStorage.getItem(COLLAPSED_UI_ALERTS_KEY)).toBeNull();
  });

  it("shows every alert expanded by default with no show-more button", () => {
    setAlerts([
      { category: "info", text: "Alert one" },
      { category: "warning", text: "Alert two" },
      { category: "error", text: "Alert three" },
    ]);

    const { container } = render(<Dashboard />, { wrapper: Wrapper });

    expect(screen.getByText("Alert one")).toBeInTheDocument();
    expect(screen.getByText("Alert two")).toBeInTheDocument();
    expect(screen.getByText("Alert three")).toBeInTheDocument();
    expect(accordionState(container)).toBe("open");
    expect(screen.queryByRole("button", { name: /more alerts/u })).not.toBeInTheDocument();
  });

  it("persists the collapsed state to localStorage when the accordion is collapsed", () => {
    setAlerts([
      { category: "info", text: "Alert one" },
      { category: "warning", text: "Alert two" },
      { category: "error", text: "Alert three" },
    ]);

    const { container } = render(<Dashboard />, { wrapper: Wrapper });

    expect(accordionState(container)).toBe("open");

    fireEvent.click(container.querySelector('[data-part="item-trigger"]') as HTMLElement);

    expect(globalThis.localStorage.getItem(COLLAPSED_UI_ALERTS_KEY)).toBe("true");
    expect(accordionState(container)).toBe("closed");
    expect(screen.getByRole("button", { name: "+2 more alerts" })).toBeVisible();
  });

  it("restores the collapsed state from localStorage on reload and surfaces a show-more button", () => {
    globalThis.localStorage.setItem(COLLAPSED_UI_ALERTS_KEY, "true");
    setAlerts([
      { category: "info", text: "Alert one" },
      { category: "warning", text: "Alert two" },
      { category: "error", text: "Alert three" },
    ]);

    const { container } = render(<Dashboard />, { wrapper: Wrapper });

    expect(accordionState(container)).toBe("closed");
    expect(screen.getByRole("button", { name: "+2 more alerts" })).toBeVisible();
  });

  it("expands and clears the persisted collapsed state when show-more is clicked", () => {
    globalThis.localStorage.setItem(COLLAPSED_UI_ALERTS_KEY, "true");
    setAlerts([
      { category: "info", text: "Alert one" },
      { category: "warning", text: "Alert two" },
    ]);

    const { container } = render(<Dashboard />, { wrapper: Wrapper });

    fireEvent.click(screen.getByRole("button", { name: "+1 more alert" }));

    expect(globalThis.localStorage.getItem(COLLAPSED_UI_ALERTS_KEY)).toBe("false");
    expect(accordionState(container)).toBe("open");
    expect(screen.queryByRole("button", { name: /more alert/u })).not.toBeInTheDocument();
  });

  it("renders a single alert without an accordion trigger or show-more button", () => {
    setAlerts([{ category: "info", text: "Only alert" }]);

    const { container } = render(<Dashboard />, { wrapper: Wrapper });

    expect(screen.getByText("Only alert")).toBeInTheDocument();
    expect(container.querySelector('[data-part="item-trigger"]')).toBeNull();
    expect(screen.queryByRole("button", { name: /more alert/u })).not.toBeInTheDocument();
    expect(globalThis.localStorage.getItem(COLLAPSED_UI_ALERTS_KEY)).toBeNull();
  });
});
