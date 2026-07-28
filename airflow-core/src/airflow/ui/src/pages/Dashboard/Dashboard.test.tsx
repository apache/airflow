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

const threeAlerts: Array<UIAlert> = [
  { category: "info", text: "Alert one" },
  { category: "warning", text: "Alert two" },
  { category: "error", text: "Alert three" },
];

describe("Dashboard alerts collapse toggle", () => {
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

  it("shows every alert expanded by default with a collapse toggle", () => {
    setAlerts(threeAlerts);

    render(<Dashboard />, { wrapper: Wrapper });

    expect(screen.getByText("Alert one")).toBeInTheDocument();
    expect(screen.getByText("Alert two")).toBeInTheDocument();
    expect(screen.getByText("Alert three")).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Show fewer alerts" })).toBeVisible();
    expect(screen.queryByText("+2 more alerts")).not.toBeInTheDocument();
  });

  it("persists the collapsed state and hides extra alerts when collapsed", () => {
    setAlerts(threeAlerts);

    render(<Dashboard />, { wrapper: Wrapper });

    fireEvent.click(screen.getByRole("button", { name: "Show fewer alerts" }));

    expect(globalThis.localStorage.getItem(COLLAPSED_UI_ALERTS_KEY)).toBe("true");
    expect(screen.getByText("Alert one")).toBeInTheDocument();
    expect(screen.queryByText("Alert two")).not.toBeInTheDocument();
    expect(screen.queryByText("Alert three")).not.toBeInTheDocument();
    expect(screen.getByText("+2 more alerts")).toBeVisible();
    expect(screen.getByRole("button", { name: "Show 2 more alerts" })).toBeVisible();
  });

  it("restores the collapsed state from localStorage on reload", () => {
    globalThis.localStorage.setItem(COLLAPSED_UI_ALERTS_KEY, "true");
    setAlerts(threeAlerts);

    render(<Dashboard />, { wrapper: Wrapper });

    expect(screen.getByText("Alert one")).toBeInTheDocument();
    expect(screen.queryByText("Alert two")).not.toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Show 2 more alerts" })).toBeVisible();
  });

  it("expands and clears the persisted collapsed state when the toggle is clicked", () => {
    globalThis.localStorage.setItem(COLLAPSED_UI_ALERTS_KEY, "true");
    setAlerts([
      { category: "info", text: "Alert one" },
      { category: "warning", text: "Alert two" },
    ]);

    render(<Dashboard />, { wrapper: Wrapper });

    fireEvent.click(screen.getByRole("button", { name: "Show 1 more alert" }));

    expect(globalThis.localStorage.getItem(COLLAPSED_UI_ALERTS_KEY)).toBe("false");
    expect(screen.getByText("Alert two")).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Show fewer alerts" })).toBeVisible();
  });

  it("renders a single alert without a collapse toggle", () => {
    setAlerts([{ category: "info", text: "Only alert" }]);

    render(<Dashboard />, { wrapper: Wrapper });

    expect(screen.getByText("Only alert")).toBeInTheDocument();
    expect(screen.queryByRole("button", { name: /alert/u })).not.toBeInTheDocument();
    expect(globalThis.localStorage.getItem(COLLAPSED_UI_ALERTS_KEY)).toBeNull();
  });
});
