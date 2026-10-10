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
import { fireEvent, render as baseRender, screen, waitFor, within } from "@testing-library/react";
import i18n from "i18next";
import { initReactI18next } from "react-i18next";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import type * as Queries from "openapi/queries";

import {
  TIMEZONE_KEY,
  TIME_SCHEDULE_AGGREGATION_MODE_KEY,
  TIME_SCHEDULE_DAG_RUN_LIMIT_KEY,
  TIME_SCHEDULE_VIEW_MODE_KEY,
} from "src/constants/localStorage";
import { AppWrapper } from "src/utils/AppWrapper";

import common from "../../../public/i18n/locales/en/common.json";
import dag from "../../../public/i18n/locales/en/dag.json";

type StreamItem = {
  readonly dag_display_name: string;
  readonly dag_id: string;
  readonly dag_run_id: string;
  readonly duration_ms: number;
  readonly end_date: string | null;
  readonly is_time_scheduled: boolean;
  readonly run_after_max: string;
  readonly run_after_min: string;
  readonly run_count: number;
  readonly start_date: string | null;
  readonly start_time_gte?: string;
  readonly start_time_lt?: string;
  readonly start_weekday?: number;
  readonly state: "failed" | "success";
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
  TIMEZONE_KEY,
  TIME_SCHEDULE_VIEW_MODE_KEY,
  TIME_SCHEDULE_AGGREGATION_MODE_KEY,
  TIME_SCHEDULE_DAG_RUN_LIMIT_KEY,
];

const createStreamItem = (overrides: Partial<StreamItem> = {}): StreamItem => ({
  dag_display_name: "example_dag",
  dag_id: "example_dag",
  dag_run_id: "run-1",
  duration_ms: 60_000,
  end_date: "2024-01-01T00:01:00Z",
  is_time_scheduled: true,
  run_after_max: "2024-01-01T00:00:00Z",
  run_after_min: "2024-01-01T00:00:00Z",
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
        dag_display_name: "another_dag",
        dag_id: "another_dag",
        dag_run_id: "run-2",
        duration_ms: 120_000,
        end_date: "2024-01-01T02:02:00Z",
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

const render = (initialEntry = "/time-schedule") =>
  baseRender(<AppWrapper initialEntries={[initialEntry]} />);

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
    resources: { en: { common, dag } },
  });
}

vi.mock("openapi/queries", async (importOriginal) => ({
  ...(await importOriginal<typeof Queries>()),
  useConfigServiceGetConfigs: () => ({ data: configResponse.current }),
  useTeamsServiceListTeams: () => ({ data: { teams: [] } }),
}));

vi.mock("src/i18n/config", async () => ({
  default: (await import("i18next")).default,
  defaultLanguage: "en",
  namespaces: [],
  supportedLanguages: [],
}));

vi.mock("src/layouts/BaseLayout", async () => {
  const { Outlet } = await import("react-router-dom");

  return { BaseLayout: () => <Outlet /> };
});

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
    // happy-dom has no layout; provide a viewport for the real row virtualizer.
    vi.spyOn(HTMLElement.prototype, "offsetHeight", "get").mockReturnValue(480);
    vi.spyOn(HTMLElement.prototype, "offsetWidth", "get").mockReturnValue(1000);
    configResponse.current.multi_team = false;
    TIME_SCHEDULE_STORAGE_KEYS.forEach((key) => globalThis.localStorage.removeItem(key));
    globalThis.localStorage.setItem(TIMEZONE_KEY, JSON.stringify("UTC"));
    fetchTimeSchedule.mockReset();
    fetchTimeSchedule.mockResolvedValue(createStreamResponse());
    vi.stubGlobal("fetch", fetchTimeSchedule);
  });

  afterEach(() => {
    TIME_SCHEDULE_STORAGE_KEYS.forEach((key) => globalThis.localStorage.removeItem(key));
    vi.unstubAllGlobals();
    vi.restoreAllMocks();
  });

  it("renders streamed batches progressively on the Day timeline", async () => {
    render();

    await waitFor(() => expect(screen.getByText("2 Dag Runs")).toBeInTheDocument());
    expect(screen.getByTestId("time-schedule-day-grid")).toBeInTheDocument();
    expect(screen.getByRole("link", { name: "example_dag: Success, 1 Dag Run" })).toHaveAttribute(
      "href",
      "/dags/example_dag/runs/run-1",
    );
    expect(screen.getByRole("link", { name: "another_dag: Failed, 1 Dag Run" })).toHaveAttribute(
      "href",
      "/dags/another_dag/runs/run-2",
    );
  });

  it("virtualizes Dag labels and bars together while scrolling", async () => {
    fetchTimeSchedule.mockResolvedValue(
      createStreamResponse([
        {
          dag_run_count: 200,
          items: Array.from({ length: 200 }, (_, index) => {
            const dagId = `dag_${String(index).padStart(3, "0")}`;

            return createStreamItem({ dag_display_name: dagId, dag_id: dagId, dag_run_id: dagId });
          }),
        },
      ]),
    );

    render();

    expect(await screen.findByRole("link", { name: "dag_000: Success, 1 Dag Run" })).toBeInTheDocument();
    expect(screen.queryByRole("link", { name: "dag_199" })).not.toBeInTheDocument();
    expect(screen.getAllByTestId(/^time-schedule-run-bar-/u).length).toBeLessThan(200);

    fireEvent.scroll(screen.getByTestId("time-schedule-scroll-region"), { target: { scrollTop: 9200 } });

    expect(await screen.findByRole("link", { name: "dag_199" })).toBeInTheDocument();
    expect(screen.getByRole("link", { name: "dag_199: Success, 1 Dag Run" })).toBeInTheDocument();
    expect(screen.queryByRole("link", { name: "dag_000" })).not.toBeInTheDocument();
  });

  it("updates virtual row heights when zooming changes overlapping lanes", async () => {
    fetchTimeSchedule.mockResolvedValue(
      createStreamResponse([
        {
          dag_run_count: 3,
          items: [
            createStreamItem(),
            createStreamItem({
              dag_run_id: "run-overlap",
              end_date: "2024-01-01T00:36:00Z",
              start_date: "2024-01-01T00:35:00Z",
              state: "failed",
            }),
            createStreamItem({ dag_display_name: "next_dag", dag_id: "next_dag", dag_run_id: "next-run" }),
          ],
        },
      ]),
    );

    render();

    const nextRow = (await screen.findByRole("link", { name: /^next_dag$/u })).parentElement;

    expect(nextRow).toHaveStyle({ top: "56px" });
    expect(screen.getByTestId("time-schedule-run-bar-next-run").parentElement).toHaveStyle({ top: "72px" });

    fireEvent.click(screen.getByRole("button", { name: "Zoom in" }));

    await waitFor(() => expect(nextRow).toHaveStyle({ top: "48px" }));
    expect(screen.getByTestId("time-schedule-run-bar-next-run").parentElement).toHaveStyle({ top: "64px" });
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
      "/time-schedule?dag_id_pattern=example&state=failed&run_type=scheduled&tags=tag-a&tags=tag-b&tags_match_mode=all&timetable_type=CronTriggerTimetable&teams=analytics&paused=true",
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
    expect(request.searchParams.get("paused")).toBe("true");
  });

  it("requests only Week data when the view changes", async () => {
    render();
    await waitFor(() => expect(screen.getByText("2 Dag Runs")).toBeInTheDocument());

    const weekTab = screen.getByRole("button", { name: "Week" });

    fireEvent.click(weekTab);

    await waitFor(() => expect(getLatestRequest().searchParams.get("view_mode")).toBe("week"));
    expect(screen.getByTestId("time-schedule-week-grid")).toBeInTheDocument();
    expect(screen.queryByTestId("time-schedule-day-grid")).not.toBeInTheDocument();
  });

  it("requests the selected aggregation without calculating the unused view", async () => {
    render();
    await waitFor(() => expect(fetchTimeSchedule).toHaveBeenCalled());

    await selectOption("time-schedule-aggregation", "Full time range");

    await waitFor(() => expect(getLatestRequest().searchParams.get("aggregation_mode")).toBe("max"));
  });

  it("limits Dag runs to bounded choices and never offers All Dag runs", async () => {
    render();
    await waitFor(() => expect(fetchTimeSchedule).toHaveBeenCalled());

    fireEvent.click(within(screen.getByTestId("time-schedule-dag-run-limit")).getByRole("combobox"));

    expect(await screen.findByRole("option", { name: "Limit 200" })).toBeInTheDocument();
    expect(screen.getByRole("option", { name: "Limit 5000" })).toBeInTheDocument();
    expect(screen.queryByRole("option", { name: /All Dag runs/u })).not.toBeInTheDocument();

    fireEvent.click(screen.getByRole("option", { name: "Limit 600" }));
    await waitFor(() => expect(getLatestRequest().searchParams.get("limit")).toBe("600"));
  });

  it("moves Scheduled Dags only filtering to the server request", async () => {
    render();
    await waitFor(() => expect(fetchTimeSchedule).toHaveBeenCalled());

    fireEvent.click(screen.getByRole("button", { name: "Remove Scheduled Dags only filter" }));

    await waitFor(() => expect(getLatestRequest().searchParams.get("show_scheduled_only")).toBe("false"));
  });

  it("links aggregated items to the runs table filtered by state, run period, and local start bucket", async () => {
    fetchTimeSchedule.mockResolvedValue(
      createStreamResponse([
        {
          dag_run_count: 2,
          items: [
            createStreamItem({
              end_date: "2024-01-01T00:23:00Z",
              run_after_max: "2024-01-02T00:00:00Z",
              run_count: 2,
              start_date: "2024-01-01T00:22:00Z",
              start_time_gte: "00:00",
              start_time_lt: "01:00",
              start_weekday: 1,
            }),
          ],
        },
      ]),
    );

    render();

    expect(await screen.findByRole("link", { name: "example_dag: Success, 2 Dag Runs" })).toHaveAttribute(
      "href",
      "/dags/example_dag/runs?run_after_gte=2024-01-01T00%3A00%3A00Z&run_after_lte=2024-01-02T00%3A00%3A00Z&state=success&start_time_gte=00%3A00%3A00.000Z&start_time_lt=01%3A00%3A00.000Z&start_weekday=1",
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
    expect(await screen.findByRole("link", { name: "example_dag: Success, 1 Dag Run" })).toBeInTheDocument();

    fireEvent.click(screen.getByRole("button", { name: "Zoom in" }));

    expect(fetchTimeSchedule).toHaveBeenCalledTimes(1);
    expect(screen.getByRole("link", { name: "example_dag: Success, 1 Dag Run" })).toBeInTheDocument();

    await waitFor(() => expect(fetchTimeSchedule).toHaveBeenCalledTimes(2));
  });

  it("uses modifier-arrow shortcuts only inside the timeline", async () => {
    render();
    await waitFor(() => expect(fetchTimeSchedule).toHaveBeenCalledTimes(1));

    fireEvent.keyDown(document.body, { code: "ArrowUp", ctrlKey: true, key: "ArrowUp" });
    expect(screen.getByText("60m")).toBeInTheDocument();

    fireEvent.keyDown(screen.getByTestId("time-schedule-chart"), {
      code: "ArrowUp",
      ctrlKey: true,
      key: "ArrowUp",
    });

    await waitFor(() => expect(getLatestRequest().searchParams.get("time_scale")).toBe("50"));
    expect(screen.getByText("50m")).toBeInTheDocument();
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

    render("/time-schedule?show_scheduled_only=false");

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
    expect(screen.getByText("0 Dag Runs")).toBeInTheDocument();
  });
});
