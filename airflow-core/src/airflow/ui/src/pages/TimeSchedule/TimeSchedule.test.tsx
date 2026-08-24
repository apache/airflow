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

import {
  TIME_SCHEDULE_AGGREGATION_MODE_KEY,
  TIME_SCHEDULE_DAG_RUN_LIMIT_KEY,
  TIME_SCHEDULE_SCHEDULED_ONLY_KEY,
  TIME_SCHEDULE_VIEW_MODE_KEY,
} from "src/constants/localStorage";
import { TimezoneContext } from "src/context/timezone";

import { TimeSchedule } from "./TimeSchedule";

type StreamItem = {
  readonly dag_id: string;
  readonly dag_run_id: string;
  readonly duration_ms: number;
  readonly end_date: string | null;
  readonly is_placeholder: boolean;
  readonly is_planned: boolean;
  readonly is_time_scheduled: boolean;
  readonly label: string;
  readonly run_count: number;
  readonly start_date: string | null;
  readonly state: "failed" | "planned" | "success";
};

type StreamBatch = {
  readonly dag_run_count: number;
  readonly items: Array<StreamItem>;
};

const { configResponse, fetchTimeSchedule } = vi.hoisted(() => ({
  configResponse: { current: { multi_team: false } },
  fetchTimeSchedule: vi.fn<(input: RequestInfo | URL, init?: RequestInit) => Promise<Response>>(),
}));

const TIME_SCHEDULE_STORAGE_KEYS = [
  TIME_SCHEDULE_VIEW_MODE_KEY,
  TIME_SCHEDULE_AGGREGATION_MODE_KEY,
  TIME_SCHEDULE_DAG_RUN_LIMIT_KEY,
  TIME_SCHEDULE_SCHEDULED_ONLY_KEY,
];

const createStreamItem = (overrides: Partial<StreamItem> = {}): StreamItem => ({
  dag_id: "example_dag",
  dag_run_id: "run-1",
  duration_ms: 60_000,
  end_date: "2024-01-01T00:01:00Z",
  is_placeholder: false,
  is_planned: false,
  is_time_scheduled: true,
  label: "example_dag",
  run_count: 1,
  start_date: "2024-01-01T00:00:00Z",
  state: "success",
  ...overrides,
});

const defaultBatches: Array<StreamBatch> = [
  { dag_run_count: 1, items: [createStreamItem()] },
  {
    dag_run_count: 1,
    items: [
      createStreamItem({
        dag_id: "another_dag",
        dag_run_id: "run-2",
        duration_ms: 120_000,
        end_date: "2024-01-01T02:02:00Z",
        label: "another_dag",
        start_date: "2024-01-01T02:00:00Z",
        state: "failed",
      }),
    ],
  },
];

const createStreamResponse = (batches: Array<StreamBatch> = defaultBatches) =>
  new Response(`${batches.map((batch) => JSON.stringify(batch)).join("\n")}\n`, {
    headers: { "Content-Type": "application/x-ndjson" },
    status: 200,
  });

const render = (initialEntry = "/time_schedule", selectedTimezone = "UTC") =>
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
  fireEvent.click(within(screen.getByTestId(selectTestId)).getByRole("combobox"));
  fireEvent.click(await screen.findByRole("option", { name: optionName }));
};

const getLatestRequest = () => {
  const request = fetchTimeSchedule.mock.calls.at(-1)?.[0];
  const requestUrl =
    typeof request === "string" ? request : request instanceof URL ? request.href : request?.url;

  return new URL(requestUrl ?? "", "http://localhost");
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
          filters: { filterByTag: "Filter by tag", timetableType: "Timetable Type" },
          states: { scheduled: "Scheduled" },
          timeSchedule: {
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
  useTeamsServiceListTeams: () => ({ data: { teams: [] } }),
}));

vi.mock("src/queries/useDagTagsInfinite", () => ({
  useDagTagsInfinite: () => ({
    data: { pages: [{ tags: ["tag-a", "tag-b"], total_entries: 2 }] },
    fetchNextPage: vi.fn(),
  }),
}));

vi.mock("src/queries/useDagTimetableTypesInfinite", () => ({
  useDagTimetableTypesInfinite: () => ({
    data: { pages: [{ timetable_types: ["CronTriggerTimetable", "NullTimetable"], total_entries: 2 }] },
    fetchNextPage: vi.fn(),
  }),
}));

describe("TimeSchedule page", () => {
  beforeEach(() => {
    configResponse.current.multi_team = false;
    TIME_SCHEDULE_STORAGE_KEYS.forEach((key) => globalThis.localStorage.removeItem(key));
    fetchTimeSchedule.mockReset();
    fetchTimeSchedule.mockResolvedValue(createStreamResponse());
    vi.stubGlobal("fetch", fetchTimeSchedule);
  });

  afterEach(() => {
    TIME_SCHEDULE_STORAGE_KEYS.forEach((key) => globalThis.localStorage.removeItem(key));
    vi.unstubAllGlobals();
  });

  it("renders streamed batches progressively on the Day timeline", async () => {
    render();

    await waitFor(() => expect(screen.getByText("2 Dag runs rendered")).toBeInTheDocument());
    expect(screen.getByTestId("time-schedule-day-grid")).toBeInTheDocument();
    expect(screen.getByRole("link", { name: "View Dag run run-1" })).toHaveAttribute(
      "href",
      "/dags/example_dag/runs/run-1",
    );
    expect(screen.getByRole("link", { name: "View Dag run run-2" })).toHaveAttribute(
      "href",
      "/dags/another_dag/runs/run-2",
    );
  });

  it("requests one server stream for the selected view and aggregation", async () => {
    render();

    await waitFor(() => expect(fetchTimeSchedule).toHaveBeenCalledTimes(1));
    const request = getLatestRequest();

    expect(request.pathname).toBe("/ui/time-schedule");
    expect(request.searchParams.get("aggregation_mode")).toBe("mean");
    expect(request.searchParams.get("limit")).toBe("200");
    expect(request.searchParams.get("show_scheduled_only")).toBe("true");
    expect(request.searchParams.get("time_scale")).toBe("60");
    expect(request.searchParams.get("timezone")).toBe("UTC");
    expect(request.searchParams.get("view_mode")).toBe("day");
  });

  it("forwards Dag run and Dag metadata filters to the server", async () => {
    configResponse.current.multi_team = true;
    render(
      "/time_schedule?dag_id_pattern=example&state=failed&run_type=scheduled&tags=tag-a&tags=tag-b&tags_match_mode=all&timetable_type=CronTriggerTimetable&teams=analytics",
    );

    await waitFor(() => expect(fetchTimeSchedule).toHaveBeenCalled());
    const request = getLatestRequest();

    expect(request.searchParams.get("dag_id_pattern")).toBe("example");
    expect(request.searchParams.get("state")).toBe("failed");
    expect(request.searchParams.get("run_type")).toBe("scheduled");
    expect(request.searchParams.getAll("tags")).toEqual(["tag-a", "tag-b"]);
    expect(request.searchParams.get("tags_match_mode")).toBe("all");
    expect(request.searchParams.get("timetable_type")).toBe("CronTriggerTimetable");
    expect(request.searchParams.getAll("teams")).toEqual(["analytics"]);
  });

  it("requests only Week data when the view changes", async () => {
    render();
    await waitFor(() => expect(screen.getByText("2 Dag runs rendered")).toBeInTheDocument());

    await selectOption("time-schedule-view-mode", "Week");

    await waitFor(() => expect(getLatestRequest().searchParams.get("view_mode")).toBe("week"));
    expect(screen.getByTestId("time-schedule-week-grid")).toBeInTheDocument();
    expect(screen.queryByTestId("time-schedule-day-grid")).not.toBeInTheDocument();
  });

  it("requests the selected aggregation without calculating the unused view", async () => {
    render();
    await waitFor(() => expect(fetchTimeSchedule).toHaveBeenCalled());

    await selectOption("time-schedule-aggregation", "Max");

    await waitFor(() => expect(getLatestRequest().searchParams.get("aggregation_mode")).toBe("max"));
  });

  it("limits Dag runs to bounded choices and never offers All Dag runs", async () => {
    render();
    await waitFor(() => expect(fetchTimeSchedule).toHaveBeenCalled());

    fireEvent.click(within(screen.getByTestId("time-schedule-dag-run-limit")).getByRole("combobox"));

    expect(await screen.findByRole("option", { name: "Latest 200" })).toBeInTheDocument();
    expect(screen.getByRole("option", { name: "Latest 5000" })).toBeInTheDocument();
    expect(screen.queryByRole("option", { name: /All Dag runs/u })).not.toBeInTheDocument();

    fireEvent.click(screen.getByRole("option", { name: "Latest 600" }));
    await waitFor(() => expect(getLatestRequest().searchParams.get("limit")).toBe("600"));
  });

  it("moves Scheduled Dags only filtering to the server request", async () => {
    render();
    await waitFor(() => expect(fetchTimeSchedule).toHaveBeenCalled());

    fireEvent.click(screen.getByRole("checkbox", { name: "Scheduled Dags only" }));

    await waitFor(() => expect(getLatestRequest().searchParams.get("show_scheduled_only")).toBe("false"));
  });

  it("renders a planned item returned by the server", async () => {
    fetchTimeSchedule.mockResolvedValue(
      createStreamResponse([
        {
          dag_run_count: 0,
          items: [
            createStreamItem({
              dag_id: "planned_dag",
              dag_run_id: "planned_dag-planned",
              is_planned: true,
              label: "planned_dag",
              run_count: 0,
              state: "planned",
            }),
          ],
        },
      ]),
    );

    render();

    expect(await screen.findByRole("link", { name: "View planned_dag Dag runs" })).toHaveAttribute(
      "href",
      "/dags/planned_dag/runs",
    );
  });

  it("keeps existing zoom behavior while requesting the matching server bucket size", async () => {
    render();
    await waitFor(() => expect(fetchTimeSchedule).toHaveBeenCalled());

    fireEvent.click(screen.getByRole("button", { name: "Zoom in" }));

    await waitFor(() => expect(getLatestRequest().searchParams.get("time_scale")).toBe("30"));
    expect(screen.getByText("30m")).toBeInTheDocument();
  });

  it("keeps rendered bars visible while zoom aggregation is debounced", async () => {
    render();
    expect(await screen.findByRole("link", { name: "View Dag run run-1" })).toBeInTheDocument();

    fireEvent.click(screen.getByRole("button", { name: "Zoom in" }));

    expect(fetchTimeSchedule).toHaveBeenCalledTimes(1);
    expect(screen.getByRole("link", { name: "View Dag run run-1" })).toBeInTheDocument();

    await waitFor(() => expect(fetchTimeSchedule).toHaveBeenCalledTimes(2));
  });

  it("requests only the final server aggregation while zooming repeatedly", async () => {
    render();
    await waitFor(() => expect(fetchTimeSchedule).toHaveBeenCalledTimes(1));

    const zoomIn = screen.getByRole("button", { name: "Zoom in" });

    fireEvent.click(zoomIn);
    fireEvent.click(zoomIn);
    fireEvent.click(zoomIn);

    expect(fetchTimeSchedule).toHaveBeenCalledTimes(1);
    await waitFor(() => expect(fetchTimeSchedule).toHaveBeenCalledTimes(2));
    expect(getLatestRequest().searchParams.get("time_scale")).toBe("10");
  });

  it("restores bounded view settings after remounting", async () => {
    globalThis.localStorage.setItem(TIME_SCHEDULE_VIEW_MODE_KEY, JSON.stringify("week"));
    globalThis.localStorage.setItem(TIME_SCHEDULE_AGGREGATION_MODE_KEY, JSON.stringify("min"));
    globalThis.localStorage.setItem(TIME_SCHEDULE_DAG_RUN_LIMIT_KEY, JSON.stringify(1000));
    globalThis.localStorage.setItem(TIME_SCHEDULE_SCHEDULED_ONLY_KEY, JSON.stringify(false));

    render();

    await waitFor(() => expect(fetchTimeSchedule).toHaveBeenCalled());
    const request = getLatestRequest();

    expect(request.searchParams.get("view_mode")).toBe("week");
    expect(request.searchParams.get("aggregation_mode")).toBe("min");
    expect(request.searchParams.get("limit")).toBe("1000");
    expect(request.searchParams.get("show_scheduled_only")).toBe("false");
  });

  it("shows a stream error without keeping the chart in its loading state", async () => {
    fetchTimeSchedule.mockResolvedValue(new Response(null, { status: 500 }));

    render();

    expect(await screen.findByText("Time Schedule request failed with status 500")).toBeInTheDocument();
    expect(screen.getByText("0 Dag runs rendered")).toBeInTheDocument();
  });

  it("does not focus the chart when opening a view control", async () => {
    render();
    await waitFor(() => expect(fetchTimeSchedule).toHaveBeenCalled());

    const chart = screen.getByTestId("time-schedule-chart");
    const limitSelect = within(screen.getByTestId("time-schedule-dag-run-limit")).getByRole("combobox");

    fireEvent.mouseDown(limitSelect);

    expect(chart).not.toHaveFocus();
  });
});
