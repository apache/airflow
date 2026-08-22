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
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import "@testing-library/jest-dom";
import { fireEvent, render as baseRender, screen, waitFor, within } from "@testing-library/react";
import i18n from "i18next";
import { initReactI18next } from "react-i18next";
import { MemoryRouter } from "react-router-dom";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import type { DAGRunResponse, DAGWithLatestDagRunsResponse } from "openapi/requests/types.gen";
import { TimezoneContext } from "src/context/timezone";

import { TimeSchedule } from "./TimeSchedule";
import { getTimelineItemColorPalette } from "./timelineUtils";

type DagRunsFixture = {
  dag_runs: Array<
    Pick<
      DAGRunResponse,
      | "dag_display_name"
      | "dag_id"
      | "dag_run_id"
      | "duration"
      | "end_date"
      | "run_after"
      | "start_date"
      | "state"
    >
  >;
};
type DagsUiFixture = {
  dags: Array<
    Pick<
      DAGWithLatestDagRunsResponse,
      "dag_display_name" | "dag_id" | "next_dagrun_run_after" | "timetable_periodic" | "timetable_summary"
    >
  >;
  total_entries: number;
};

const { configResponse, dagRunsResponse, dagsUiResponse, getDagDetails, getDagRuns, getDagsUi } = vi.hoisted(
  () => ({
    configResponse: { current: { fallback_page_limit: 100, multi_team: false } },
    dagRunsResponse: { current: {} as DagRunsFixture },
    dagsUiResponse: { current: {} as DagsUiFixture },
    getDagDetails: vi.fn(),
    getDagRuns: vi.fn(),
    getDagsUi: vi.fn(),
  }),
);

const TIME_SCHEDULE_STORAGE_KEYS = [
  "time-schedule-view-mode",
  "time-schedule-aggregation-mode",
  "time-schedule-dag-run-limit",
  "time-schedule-scheduled-only",
];

const render = (selectedTimezone: string = "UTC", initialEntry: string = "/time_schedule") =>
  baseRender(
    <QueryClientProvider client={new QueryClient()}>
      <TimezoneContext.Provider value={{ selectedTimezone, setSelectedTimezone: vi.fn() }}>
        <ChakraProvider value={defaultSystem}>
          <MemoryRouter initialEntries={[initialEntry]}>
            <TimeSchedule />
          </MemoryRouter>
        </ChakraProvider>
      </TimezoneContext.Provider>
    </QueryClientProvider>,
  );

const selectOption = async (selectTestId: string, optionName: string) => {
  const trigger = within(screen.getByTestId(selectTestId)).getByRole("combobox");

  fireEvent.click(trigger);
  fireEvent.click(await screen.findByRole("option", { name: optionName }));
};

if (!i18n.isInitialized) {
  void i18n.use(initReactI18next).init({
    defaultNS: "common",
    fallbackLng: "en",
    lng: "en",
    resources: {
      en: {
        common: {
          dagId: "Dag ID",
          states: { scheduled: "Scheduled" },
          timeSchedule: {
            all: "All",
            allDagRuns: "All Dag runs",
            dagRuns: "{{count}} Dag runs",
            dagRunsRendered: "{{count}} Dag runs rendered",
            dagRunsToDisplay: "Dag runs to display",
            day: "Day",
            durationAggregation: "Duration aggregation",
            latestDagRuns: "Latest {{count}}",
            loading: "Loading...",
            max: "Max",
            mean: "Mean",
            min: "Min",
            minutes: "{{value}}m",
            nextRun: "Next run: {{time}}",
            scheduledDagsOnly: "Scheduled Dags only",
            title: "Time Schedule",
            viewMode: "View mode",
            week: "Week",
            weekday: {
              Fri: "Fri",
              Mon: "Mon",
              Sat: "Sat",
              Sun: "Sun",
              Thu: "Thu",
              Tue: "Tue",
              Wed: "Wed",
            },
            zoomIn: "Zoom in",
            zoomOut: "Zoom out",
          },
        },
      },
    },
  });
}

vi.mock("openapi/queries", () => ({
  useConfigServiceGetConfigs: () => ({ data: configResponse.current }),
  useDagRunServiceGetDagRuns: (params: unknown) => {
    getDagRuns(params);

    return { data: dagRunsResponse.current, isLoading: false };
  },
  useDagServiceGetDagsUi: (params: unknown) => {
    getDagsUi(params);

    return { data: dagsUiResponse.current, isLoading: false };
  },
  useTeamsServiceListTeams: () => ({ data: { teams: [] } }),
}));

vi.mock("src/queries/useDagTagsInfinite", () => ({
  useDagTagsInfinite: () => ({
    data: { pages: [{ tags: ["tag-a", "tag-b"], total_entries: 2 }] },
    fetchNextPage: vi.fn(),
    fetchPreviousPage: vi.fn(),
  }),
}));

vi.mock("src/queries/useDagTimetableTypesInfinite", () => ({
  useDagTimetableTypesInfinite: () => ({
    data: { pages: [{ timetable_types: ["CronTriggerTimetable", "NullTimetable"], total_entries: 2 }] },
    fetchNextPage: vi.fn(),
    fetchPreviousPage: vi.fn(),
  }),
}));

vi.mock("openapi/requests/services.gen", () => ({
  DagRunService: { getDagRuns },
  DagService: { getDagDetails, getDagsUi },
}));

describe("TimeSchedule page", () => {
  beforeEach(() => {
    configResponse.current.multi_team = false;
    TIME_SCHEDULE_STORAGE_KEYS.forEach((key) => globalThis.localStorage.removeItem(key));
    getDagsUi.mockReset();
    getDagRuns.mockReset();
    getDagDetails.mockReset();
    getDagDetails.mockImplementation(({ dagId }: { dagId: string }) =>
      Promise.resolve({ dag_id: dagId, dag_run_timeout: dagId === "example_dag" ? "PT1H" : null }),
    );
    dagsUiResponse.current = {
      dags: [
        {
          dag_display_name: "example_dag",
          dag_id: "example_dag",
          next_dagrun_run_after: "2024-01-01T00:00:00Z",
          timetable_periodic: true,
          timetable_summary: "@daily",
        },
        {
          dag_display_name: "another_dag",
          dag_id: "another_dag",
          next_dagrun_run_after: "2024-01-01T02:00:00Z",
          timetable_periodic: true,
          timetable_summary: "@daily",
        },
      ],
      total_entries: 2,
    };
    dagRunsResponse.current = {
      dag_runs: [
        {
          dag_display_name: "example_dag",
          dag_id: "example_dag",
          dag_run_id: "run-1",
          duration: 60,
          end_date: "2024-01-01T00:01:00Z",
          run_after: "2024-01-01T00:00:00Z",
          start_date: "2024-01-01T00:00:00Z",
          state: "success",
        },
        {
          dag_display_name: "another_dag",
          dag_id: "another_dag",
          dag_run_id: "run-2",
          duration: 120,
          end_date: "2024-01-01T02:02:00Z",
          run_after: "2024-01-01T02:00:00Z",
          start_date: "2024-01-01T02:00:00Z",
          state: "failed",
        },
      ],
    };
  });

  afterEach(() => {
    TIME_SCHEDULE_STORAGE_KEYS.forEach((key) => globalThis.localStorage.removeItem(key));
  });

  it("renders all Dag runs on a 24-hour timeline", async () => {
    render();

    await waitFor(() => expect(screen.getByText("2 Dag runs rendered")).toBeInTheDocument());
    expect(screen.getByTestId("time-schedule-day-grid")).toBeInTheDocument();
    expect(screen.getAllByText("00:00")[0]).toBeInTheDocument();
    expect(screen.getAllByText("24:00")[0]).toBeInTheDocument();
    expect(screen.getByRole("link", { name: "example_dag" })).toHaveAttribute("href", "/dags/example_dag");
    expect(screen.getByRole("link", { name: "View Dag run run-1" })).toHaveAttribute(
      "href",
      "/dags/example_dag/runs/run-1",
    );
  });

  it("renders dotted hourly grid lines and solid six-hour lines", async () => {
    render();

    await waitFor(() => expect(screen.getByText("2 Dag runs rendered")).toBeInTheDocument());
    expect(screen.getByTestId("time-schedule-grid-line-60").firstElementChild).toHaveStyle({
      borderLeftStyle: "dotted",
    });
    expect(screen.getByTestId("time-schedule-grid-line-360").firstElementChild).toHaveStyle({
      borderLeftStyle: "solid",
    });
  });

  it("centers the first and last time labels on the padded chart edges", () => {
    render();

    const labels = screen.getAllByText("00:00");

    expect(labels[0]).toHaveStyle({ transform: "translateX(-50%)" });
    expect(screen.getByText("24:00")).toHaveStyle({ transform: "translateX(-50%)" });
  });

  it("recalculates time labels when returning from week view to day view", async () => {
    const originalClientWidth = Object.getOwnPropertyDescriptor(HTMLElement.prototype, "clientWidth");

    Object.defineProperty(HTMLElement.prototype, "clientWidth", { configurable: true, value: 1200 });

    render();

    await selectOption("time-schedule-view-mode", "Week");
    await selectOption("time-schedule-view-mode", "Day");

    await waitFor(() => expect(screen.getByText("12:00")).toBeInTheDocument());

    if (originalClientWidth) {
      Object.defineProperty(HTMLElement.prototype, "clientWidth", originalClientWidth);
    } else {
      delete (HTMLElement.prototype as { clientWidth?: number }).clientWidth;
    }
  });

  it("uses state colors and a visible minimum width for runs", () => {
    render();

    expect(getTimelineItemColorPalette({ isPlanned: false, state: "failed" })).toBe("failed");
    expect(screen.getByTestId("time-schedule-run-bar-run-1")).toHaveStyle({
      width: "max(42px, 0.06944444444444445%)",
    });
  });

  it("uses 200 Dag runs by default and lets users increase the display limit", async () => {
    render();

    expect(within(screen.getByTestId("time-schedule-dag-run-limit")).getByRole("combobox")).toHaveTextContent(
      "Latest 200",
    );
    expect(getDagRuns).toHaveBeenLastCalledWith(expect.objectContaining({ limit: 100 }));

    await selectOption("time-schedule-dag-run-limit", "Latest 1000");

    expect(within(screen.getByTestId("time-schedule-dag-run-limit")).getByRole("combobox")).toHaveTextContent(
      "Latest 1000",
    );
    await selectOption("time-schedule-dag-run-limit", "All Dag runs");
    expect(within(screen.getByTestId("time-schedule-dag-run-limit")).getByRole("combobox")).toHaveTextContent(
      "All Dag runs",
    );
  });

  it("does not apply a date range unless the user selects one", () => {
    render();

    expect(getDagRuns).toHaveBeenLastCalledWith(
      expect.objectContaining({
        startDateGte: undefined,
        startDateLte: undefined,
      }),
    );
  });

  it("applies Dag ID and team filters to the Dag metadata request", () => {
    configResponse.current.multi_team = true;

    render("UTC", "/time_schedule?dag_id_pattern=example&teams=team-a");

    expect(getDagsUi).toHaveBeenLastCalledWith(
      expect.objectContaining({ dagIdPattern: "example", teams: ["team-a"] }),
    );
  });

  it("aggregates runs that start in the same minute using the selected duration rule", async () => {
    dagRunsResponse.current = {
      dag_runs: [
        {
          dag_display_name: "example_dag",
          dag_id: "example_dag",
          dag_run_id: "run-1",
          duration: 60,
          end_date: "2024-01-01T00:01:05Z",
          run_after: "2024-01-01T00:00:00Z",
          start_date: "2024-01-01T00:00:05Z",
          state: "success",
        },
        {
          dag_display_name: "example_dag",
          dag_id: "example_dag",
          dag_run_id: "run-2",
          duration: 120,
          end_date: "2024-01-02T00:02:55Z",
          run_after: "2024-01-02T00:00:00Z",
          start_date: "2024-01-02T00:00:55Z",
          state: "success",
        },
      ],
    };

    render();

    expect(screen.getByTestId("time-schedule-run-bar-run-1")).toHaveStyle({
      width: "max(42px, 0.10416666666666667%)",
    });
    expect(screen.queryByTestId("time-schedule-run-bar-run-2")).not.toBeInTheDocument();

    await selectOption("time-schedule-aggregation", "Max");

    expect(screen.getByTestId("time-schedule-run-bar-run-1")).toHaveStyle({
      width: "max(42px, 0.1388888888888889%)",
    });
  });

  it("renders a planned scheduled Dag using its dagrun timeout", async () => {
    dagRunsResponse.current = { dag_runs: [] };
    dagsUiResponse.current = {
      dags: [
        {
          dag_display_name: "planned_dag",
          dag_id: "planned_dag",
          next_dagrun_run_after: "2024-01-01T12:00:00Z",
          timetable_periodic: true,
          timetable_summary: "@daily",
        },
      ],
      total_entries: 1,
    };
    getDagDetails.mockResolvedValue({ dag_id: "planned_dag", dag_run_timeout: "PT1H1M" });

    render();

    await waitFor(() =>
      expect(screen.getByTestId("time-schedule-run-bar-planned_dag-planned")).toHaveStyle({
        width: "max(42px, 4.236111111111112%)",
      }),
    );
    expect(screen.getByRole("link", { name: "View planned_dag Dag runs" })).toHaveAttribute(
      "href",
      "/dags/planned_dag/runs",
    );
  });

  it("hides unscheduled Dag runs by default and shows them when unchecked", () => {
    dagsUiResponse.current = {
      dags: dagsUiResponse.current.dags.slice(0, 1),
      total_entries: 1,
    };

    render();

    expect(screen.getByTestId("time-schedule-run-bar-run-1")).toBeInTheDocument();
    expect(screen.queryByTestId("time-schedule-run-bar-run-2")).not.toBeInTheDocument();

    fireEvent.click(screen.getByRole("checkbox", { name: "Scheduled Dags only" }));

    expect(screen.getByTestId("time-schedule-run-bar-run-2")).toBeInTheDocument();
  });

  it("positions runs in the selected timezone", () => {
    render("Asia/Seoul");

    expect(screen.getByTestId("time-schedule-run-bar-run-1")).toHaveStyle({ left: "37.5%" });
  });

  it("sorts Dag rows and synchronizes the timeline header", () => {
    render();

    const sortButton = screen.getByRole("button", { name: "Sort Dag ID: dagIdAscending" });
    const dagLinks = () =>
      screen
        .getAllByRole("link")
        .filter((link) => link.getAttribute("href")?.match(/^\/dags\/[^/]+$/u))
        .map((link) => link.textContent);

    expect(dagLinks()).toEqual(["another_dag", "example_dag"]);
    fireEvent.click(sortButton);
    expect(dagLinks()).toEqual(["example_dag", "another_dag"]);

    const chartBody = screen.getByTestId("time-schedule-chart-body");
    const header = screen.getByTestId("time-schedule-header-row");

    chartBody.scrollLeft = 120;
    fireEvent.scroll(chartBody);
    expect(header.scrollLeft).toBe(120);
  });

  it("zooms the chart with controls", () => {
    render();

    fireEvent.click(screen.getByRole("button", { name: "Zoom in" }));
    expect(screen.getByText("30m")).toBeInTheDocument();
    expect(screen.getByTestId("time-schedule-chart-body").firstElementChild).toHaveStyle({
      minWidth: "1920px",
    });
  });

  it("offers Dag run limits aligned with the API page size", async () => {
    render();

    await selectOption("time-schedule-dag-run-limit", "Latest 600");

    expect(within(screen.getByTestId("time-schedule-dag-run-limit")).getByRole("combobox")).toHaveTextContent(
      "Latest 600",
    );
  });

  it("only handles keyboard zoom from within the chart", () => {
    render();

    fireEvent.keyDown(document.body, { ctrlKey: true, key: "ArrowUp" });
    expect(screen.getByText("60m")).toBeInTheDocument();

    fireEvent.keyDown(screen.getByTestId("time-schedule-chart"), { ctrlKey: true, key: "ArrowUp" });
    expect(screen.getByText("50m")).toBeInTheDocument();
  });

  it("restores the selected view, aggregation, Dag run limit, and scheduled Dag filter after remounting", async () => {
    const { unmount } = render();

    await selectOption("time-schedule-view-mode", "Week");
    await selectOption("time-schedule-aggregation", "Max");
    await selectOption("time-schedule-dag-run-limit", "Latest 1000");
    fireEvent.click(screen.getByRole("checkbox", { name: "Scheduled Dags only" }));
    unmount();

    render();

    expect(within(screen.getByTestId("time-schedule-view-mode")).getByRole("combobox")).toHaveTextContent(
      "Week",
    );
    expect(within(screen.getByTestId("time-schedule-aggregation")).getByRole("combobox")).toHaveTextContent(
      "Max",
    );
    expect(within(screen.getByTestId("time-schedule-dag-run-limit")).getByRole("combobox")).toHaveTextContent(
      "Latest 1000",
    );
    expect(screen.getByRole("checkbox", { name: "Scheduled Dags only" })).not.toBeChecked();
    expect(screen.getByText("60m")).toBeInTheDocument();
  });

  it("renders aggregated Week runs in weekday and time cells", async () => {
    render();

    await selectOption("time-schedule-view-mode", "Week");

    expect(screen.getByTestId("time-schedule-week-grid")).toBeInTheDocument();
    expect(screen.getByTestId("time-schedule-week-header")).toContainElement(screen.getByText("Sun"));
    expect(screen.getByTestId("time-schedule-week-body")).not.toContainElement(screen.getByText("Sun"));
    expect(screen.getByText("Sun")).toBeInTheDocument();
    expect(screen.getByTestId("time-schedule-week-bar-run-1")).toHaveTextContent("example_dag");
    expect(screen.getByTestId("time-schedule-week-bar-run-1")).toHaveStyle({ height: "20px" });
    expect(screen.getByRole("link", { name: "View Dag run run-1" })).toHaveAttribute(
      "href",
      "/dags/example_dag/runs/run-1",
    );

    const weekBody = screen.getByTestId("time-schedule-week-body");
    const weekHeader = screen.getByTestId("time-schedule-week-header");

    weekBody.scrollLeft = 120;
    fireEvent.scroll(weekBody);
    expect(weekHeader.scrollLeft).toBe(120);

    fireEvent.click(screen.getByRole("button", { name: "Zoom in" }));

    expect(screen.getByText("30m")).toBeInTheDocument();
  });

  it("places overlapping Week runs in separate columns", async () => {
    dagRunsResponse.current = {
      dag_runs: [
        {
          dag_display_name: "example_dag",
          dag_id: "example_dag",
          dag_run_id: "overlap-run-1",
          duration: 7200,
          end_date: "2024-01-01T02:00:00Z",
          run_after: "2024-01-01T00:00:00Z",
          start_date: "2024-01-01T00:00:00Z",
          state: "success",
        },
        {
          dag_display_name: "another_dag",
          dag_id: "another_dag",
          dag_run_id: "overlap-run-2",
          duration: 7200,
          end_date: "2024-01-01T03:00:00Z",
          run_after: "2024-01-01T01:00:00Z",
          start_date: "2024-01-01T01:00:00Z",
          state: "failed",
        },
      ],
    };
    render();

    await selectOption("time-schedule-view-mode", "Week");

    expect(screen.getByTestId("time-schedule-week-bar-overlap-run-1")).toHaveStyle({
      left: "calc(0% + 2px)",
      width: "calc(50% - 4px)",
    });
    expect(screen.getByTestId("time-schedule-week-bar-overlap-run-2")).toHaveStyle({
      left: "calc(50% + 2px)",
      width: "calc(50% - 4px)",
    });
  });

  it("zooms the Week time axis around the pointer position", async () => {
    render();

    await selectOption("time-schedule-view-mode", "Week");

    const weekBody = screen.getByTestId("time-schedule-week-body");

    Object.defineProperty(weekBody, "clientHeight", { configurable: true, value: 400 });
    Object.defineProperty(weekBody, "scrollHeight", {
      configurable: true,
      get: () => (screen.queryByText("30m") ? 1920 : 960),
    });
    vi.spyOn(weekBody, "getBoundingClientRect").mockReturnValue({
      bottom: 500,
      height: 400,
      left: 0,
      right: 1000,
      toJSON: () => ({}),
      top: 100,
      width: 1000,
      x: 0,
      y: 100,
    });
    weekBody.scrollTop = 100;

    fireEvent.mouseMove(weekBody, { clientX: 500, clientY: 300 });
    fireEvent.click(screen.getByRole("button", { name: "Zoom in" }));

    await waitFor(() => expect(screen.getByText("30m")).toBeInTheDocument());
    expect(weekBody.scrollTop).toBe(400);
  });
});
