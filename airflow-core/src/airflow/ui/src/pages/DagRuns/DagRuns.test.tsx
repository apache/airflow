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
import { fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";

import * as queries from "openapi/queries";

import { TIMEZONE_KEY } from "src/constants/localStorage";
import i18n from "src/i18n/config";
import { AppWrapper } from "src/utils/AppWrapper";

describe("DagRuns recurring start filters", () => {
  afterEach(() => {
    vi.restoreAllMocks();
    localStorage.removeItem(TIMEZONE_KEY);
  });

  it("keeps Start Date and End Date next to each other in the filter menu", async () => {
    render(<AppWrapper initialEntries={["/dag_runs"]} />);
    fireEvent.click(await screen.findByRole("button", { name: i18n.t("common:filters.addFilter") }));
    await screen.findByRole("menuitem", { name: i18n.t("common:startDate") });
    const labels = screen.getAllByRole("menuitem").map((item) => item.textContent);

    expect(labels[labels.indexOf(i18n.t("common:startDate")) + 1]).toBe(i18n.t("common:endDate"));
  });

  it("passes bucket filters to the API and clears them through the existing FilterBar", async () => {
    const querySpy = vi.spyOn(queries, "useDagRunServiceGetDagRuns");

    localStorage.setItem(TIMEZONE_KEY, JSON.stringify("Asia/Seoul"));

    render(
      <AppWrapper
        initialEntries={[
          "/dag_runs?start_time_gte=02%3A00%3A00.000Z&start_time_lt=03%3A00%3A00.000Z&start_weekday=0&start_weekday=1",
        ]}
      />,
    );

    await waitFor(() =>
      expect(querySpy.mock.calls.at(-1)?.[0]).toEqual(
        expect.objectContaining({
          startTimeGte: "02:00:00.000Z",
          startTimeLt: "03:00:00.000Z",
          startWeekday: [0, 1],
        }),
      ),
    );
    expect(await screen.findByTestId("start_time_range-pill")).toHaveTextContent("11:00 - 12:00");
    fireEvent.click(screen.getByTestId("start_time_range-pill"));
    expect(await screen.findByRole("dialog")).toHaveTextContent("Asia/Seoul");
    fireEvent.change(screen.getByRole("textbox", { name: i18n.t("common:table.to") }), {
      target: { value: "13:00" },
    });
    await waitFor(() => expect(querySpy.mock.calls.at(-1)?.[0]?.startTimeLt).toBe("04:00:00.000Z"));
    fireEvent.change(screen.getByRole("textbox", { name: i18n.t("common:table.to") }), {
      target: { value: "25:00" },
    });
    expect(
      screen.getByText(i18n.t("components:dateRangeFilter.validation.invalidTimeFormat")),
    ).toBeInTheDocument();
    expect(querySpy.mock.calls.at(-1)?.[0]?.startTimeLt).toBe("04:00:00.000Z");
    fireEvent.keyDown(screen.getByRole("dialog"), { key: "Escape" });
    expect(await screen.findByTestId("start_time_range-pill")).toHaveTextContent("11:00 - 13:00");
    fireEvent.click(
      await screen.findByRole("button", {
        name: new RegExp(`^Remove ${i18n.t("common:filters.startTime")} filter$`, "u"),
      }),
    );
    await waitFor(() => expect(querySpy.mock.calls.at(-1)?.[0]?.startTimeLt).toBeUndefined());
    expect(querySpy.mock.calls.at(-1)?.[0]?.startTimeGte).toBeUndefined();
    const weekdayPill = screen.getByTestId("start_weekday-pill");

    expect(weekdayPill).toHaveTextContent(i18n.t("dag:calendar.weekdays.sunday"));
    expect(weekdayPill).toHaveTextContent(i18n.t("dag:calendar.weekdays.monday"));
    fireEvent.click(weekdayPill);
    const weekdaySelect = await screen.findByRole("combobox", { name: i18n.t("common:startWeekday") });

    fireEvent.keyDown(weekdaySelect, { key: "ArrowDown" });
    fireEvent.click(await screen.findByRole("option", { name: i18n.t("dag:calendar.weekdays.tuesday") }));
    await waitFor(() => expect(querySpy.mock.calls.at(-1)?.[0]?.startWeekday).toEqual([0, 1, 2]));
    fireEvent.click(screen.getByRole("button", { name: new RegExp(`^${i18n.t("common:reset")}$`, "u") }));
    await waitFor(() =>
      expect(querySpy.mock.calls.at(-1)?.[0]).toEqual(
        expect.objectContaining({
          startTimeGte: undefined,
          startWeekday: undefined,
        }),
      ),
    );
  });

  it("displays a UTC range crossing midnight in the selected timezone", async () => {
    localStorage.setItem(TIMEZONE_KEY, JSON.stringify("Asia/Seoul"));
    render(
      <AppWrapper
        initialEntries={["/dag_runs?start_time_gte=23%3A00%3A00.000Z&start_time_lt=01%3A00%3A00.000Z"]}
      />,
    );

    expect(await screen.findByTestId("start_time_range-pill")).toHaveTextContent("08:00 - 10:00");
    fireEvent.click(screen.getByTestId("start_time_range-pill"));
    expect(await screen.findByRole("textbox", { name: i18n.t("common:table.from") })).toHaveValue("08:00");
    expect(screen.getByRole("textbox", { name: i18n.t("common:table.to") })).toHaveValue("10:00");
  });

  it.each([
    { end: "", label: "23:00 - …", start: "23:00" },
    { end: "", label: "11:00 - …", start: "11:00" },
    { end: "12:00", label: "… - 12:00", start: "" },
  ])("shows and removes the time range $label", async ({ end, label, start }) => {
    localStorage.setItem(TIMEZONE_KEY, JSON.stringify("UTC"));
    const querySpy = vi.spyOn(queries, "useDagRunServiceGetDagRuns");
    const getListQuery = () => {
      const calls = querySpy.mock.calls.filter(([options]) => "startTimeGte" in options);

      return calls[calls.length - 1]?.[0];
    };
    const params = new URLSearchParams();

    if (start !== "") {
      params.set("start_time_gte", `${start}:00.000Z`);
    }
    if (end !== "" && end !== "24:00") {
      params.set("start_time_lt", `${end}:00.000Z`);
    }
    render(<AppWrapper initialEntries={[`/dag_runs?${params.toString()}`]} />);
    expect(await screen.findByTestId("start_time_range-pill")).toHaveTextContent(label);
    fireEvent.click(
      screen.getByRole("button", {
        name: new RegExp(`^Remove ${i18n.t("common:filters.startTime")} filter$`, "u"),
      }),
    );
    await waitFor(() =>
      expect(getListQuery()).toEqual(
        expect.objectContaining({
          startTimeGte: undefined,
          startTimeLt: undefined,
        }),
      ),
    );
  });
});

// Stand in for the Monaco-backed JSON viewer so the test can assert the collapse
// state without loading the editor.
vi.mock("src/components/RenderedJsonField", () => ({
  default: ({ collapsed }: { readonly collapsed?: boolean }) => (
    <div data-collapsed={collapsed} data-testid="rendered-json-field" />
  ),
}));

// The dag_runs mock handler (see src/mocks/handlers/dag_runs.ts) returns:
//   - run_before_filter (logical_date: 2024-12-31) — excluded when filtering Jan 2025
//   - run_in_range      (logical_date: 2025-01-15) — included when filtering Jan 2025
describe("DagRuns logical date filter", () => {
  it("shows all runs when no logical date filter is applied", async () => {
    render(<AppWrapper initialEntries={["/dag_runs"]} />);

    await waitFor(() => expect(screen.getByText("run_in_range")).toBeInTheDocument());
    expect(screen.getByText("run_before_filter")).toBeInTheDocument();
  });

  it("filters runs by logical_date_gte and logical_date_lte URL params", async () => {
    render(
      <AppWrapper
        initialEntries={[
          "/dag_runs?logical_date_gte=2025-01-01T00%3A00%3A00Z&logical_date_lte=2025-01-31T23%3A59%3A59Z",
        ]}
      />,
    );

    await waitFor(() => expect(screen.getByText("run_in_range")).toBeInTheDocument());
    expect(screen.queryByText("run_before_filter")).not.toBeInTheDocument();
  });
});

// dag_runs mock handler (see src/mocks/handlers/dag_runs.ts) tags "tagged_dag" with
// "example_tag" and "multi_tagged_dag" with "example_tag" and "other_tag"; "test_dag" has no tags.
describe("DagRuns tags filter", () => {
  it("filters runs by the tags query param", async () => {
    render(<AppWrapper initialEntries={["/dag_runs?tags=example_tag"]} />);

    await waitFor(() => expect(screen.getByText("run_tagged_dag")).toBeInTheDocument());
    expect(screen.getByText("run_multi_tagged_dag")).toBeInTheDocument();
    expect(screen.queryByText("run_in_range")).not.toBeInTheDocument();
    expect(screen.queryByText("run_before_filter")).not.toBeInTheDocument();
  });

  it("matches runs of Dags with any of the tags by default", async () => {
    render(<AppWrapper initialEntries={["/dag_runs?tags=example_tag&tags=other_tag"]} />);

    await waitFor(() => expect(screen.getByText("run_tagged_dag")).toBeInTheDocument());
    expect(screen.getByText("run_multi_tagged_dag")).toBeInTheDocument();
  });

  it("matches only runs of Dags with all of the tags when tags_match_mode is all", async () => {
    render(<AppWrapper initialEntries={["/dag_runs?tags=example_tag&tags=other_tag&tags_match_mode=all"]} />);

    await waitFor(() => expect(screen.getByText("run_multi_tagged_dag")).toBeInTheDocument());
    expect(screen.queryByText("run_tagged_dag")).not.toBeInTheDocument();
  });
});

describe("DagRuns logical date column", () => {
  afterEach(() => {
    globalThis.localStorage.clear();
  });

  it("hides the logical date column by default", async () => {
    render(<AppWrapper initialEntries={["/dag_runs"]} />);

    await waitFor(() => expect(screen.getByText("run_in_range")).toBeInTheDocument());
    expect(screen.queryByTestId("table-cell-logical_date")).not.toBeInTheDocument();
  });

  it("renders the logical date once the column is enabled", async () => {
    globalThis.localStorage.setItem(
      "dataTable:common:dagRun:columnVisibility",
      JSON.stringify({ logical_date: true }),
    );

    render(<AppWrapper initialEntries={["/dag_runs"]} />);

    await waitFor(() => expect(screen.getByText("run_in_range")).toBeInTheDocument());

    const cells = screen.getAllByTestId("table-cell-logical_date");

    expect(cells.length).toBeGreaterThan(0);
    expect(within(cells[0] as HTMLElement).getByTestId("time-display")).toBeInTheDocument();
  });
});

describe("DagRuns conf expand/collapse", () => {
  // Relies on the conf column being visible by default, which is what renders the JSON viewer.
  // useTableURLState persists sorting to localStorage, so clear it between cases.
  afterEach(() => {
    globalThis.localStorage.clear();
  });

  it("toggles conf JSON collapse state via the expand/collapse all buttons", async () => {
    render(<AppWrapper initialEntries={["/dag_runs"]} />);

    await waitFor(() => expect(screen.getByTestId("rendered-json-field")).toBeInTheDocument());

    expect(screen.getByTestId("rendered-json-field")).toHaveAttribute("data-collapsed", "true");

    fireEvent.click(screen.getByTestId("expand-all-button"));
    await waitFor(() =>
      expect(screen.getByTestId("rendered-json-field")).toHaveAttribute("data-collapsed", "false"),
    );

    fireEvent.click(screen.getByTestId("collapse-all-button"));
    await waitFor(() =>
      expect(screen.getByTestId("rendered-json-field")).toHaveAttribute("data-collapsed", "true"),
    );
  });

  it("hides the expand/collapse buttons when no runs match the filter", async () => {
    render(
      <AppWrapper
        initialEntries={[
          "/dag_runs?logical_date_gte=2030-01-01T00%3A00%3A00Z&logical_date_lte=2030-01-31T23%3A59%3A59Z",
        ]}
      />,
    );

    await waitFor(() => expect(screen.getByTestId("table-no-rows")).toBeInTheDocument());

    expect(screen.queryByTestId("expand-all-button")).toBeNull();
    expect(screen.queryByTestId("collapse-all-button")).toBeNull();
  });
});
