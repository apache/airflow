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
import dayjs from "dayjs";
import { MemoryRouter, Route, Routes, useLocation } from "react-router-dom";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { EventLogService, TaskInstanceService } from "openapi/requests";

import { TimezoneProvider } from "src/context/timezone";
import { BaseWrapper } from "src/utils/Wrapper";

import { Events } from "./Events";

const NOW = new Date("2026-11-06T12:00:00.000Z");

vi.mock("src/queries/useConfig", () => ({
  useConfig: (key: string) => key === "fallback_page_limit" && 50,
}));

let latestSearch = "";

const SearchProbe = () => {
  const location = useLocation();

  latestSearch = location.search;

  return null;
};

const renderEvents = (initialEntry: string, path: string) =>
  render(
    <BaseWrapper>
      <TimezoneProvider>
        <MemoryRouter initialEntries={[initialEntry]}>
          <Routes>
            <Route
              element={
                <>
                  <SearchProbe />
                  <Events />
                </>
              }
              path={path}
            />
          </Routes>
        </MemoryRouter>
      </TimezoneProvider>
    </BaseWrapper>,
  );

const showAllButton = () => screen.queryByRole("button", { name: /showAll|Show all/u });

beforeEach(() => {
  localStorage.clear();
  sessionStorage.clear();
  latestSearch = "";
  vi.useFakeTimers({ toFake: ["Date"] });
  vi.setSystemTime(NOW);
});

afterEach(() => {
  vi.useRealTimers();
  vi.restoreAllMocks();
  localStorage.clear();
  sessionStorage.clear();
});

const expectedAfter = () => dayjs(NOW).subtract(7, "day").toISOString();

const mockEventLogs = () =>
  vi.spyOn(EventLogService, "getEventLogs").mockResolvedValue({ event_logs: [], total_entries: 0 });

describe("browse audit log", () => {
  it("opens on the last 7 days, newest first, and says so", async () => {
    const events = mockEventLogs();

    renderEvents("/events", "/events");

    await waitFor(() => expect(events).toHaveBeenCalled());
    expect(events.mock.calls.length).toBeGreaterThan(0);
    expect(events.mock.calls.every((call) => call[0]?.after === expectedAfter())).toBe(true);
    expect(events.mock.lastCall?.[0]?.orderBy).toEqual(["-when"]);
    expect(showAllButton()).toBeInTheDocument();
    await waitFor(() => {
      expect(latestSearch).toContain("sort=-when");
      expect(decodeURIComponent(latestSearch)).toContain(expectedAfter());
    });

    const whenHeader = screen.getByRole("columnheader", { name: /when/iu });

    expect(within(whenHeader).getByLabelText("sortedDescending")).toBeInTheDocument();
    expect(screen.getByTestId("event_date_range-pill-remove")).toHaveAccessibleName(/when/iu);
  });

  it("keeps the default notice when the seeded address is opened again", async () => {
    mockEventLogs();
    const first = renderEvents("/events", "/events");

    await waitFor(() => expect(decodeURIComponent(latestSearch)).toContain(expectedAfter()));
    const seeded = latestSearch;

    first.unmount();
    renderEvents(`/events${seeded}`, "/events");

    expect(showAllButton()).toBeInTheDocument();
  });

  it("drops the week and remembers that choice from Show all", async () => {
    const events = mockEventLogs();

    renderEvents("/events", "/events");
    await waitFor(() => expect(showAllButton()).toBeInTheDocument());

    fireEvent.click(showAllButton() as HTMLElement);

    await waitFor(() => expect(events.mock.lastCall?.[0]?.after).toBeUndefined());
    expect(latestSearch).toContain("all_events=1");
    expect(latestSearch).not.toContain("after=");
    expect(showAllButton()).not.toBeInTheDocument();
  });

  it("drops the week when the When filter is removed", async () => {
    const events = mockEventLogs();

    renderEvents("/events", "/events");
    await waitFor(() => expect(screen.getByTestId("event_date_range-pill-remove")).toBeInTheDocument());

    // The pill's update is debounced with Date.now(), which stays put while the clock is frozen.
    vi.useRealTimers();
    fireEvent.click(screen.getByTestId("event_date_range-pill-remove"));

    await waitFor(() => {
      expect(latestSearch).toContain("all_events=1");
      expect(events.mock.lastCall?.[0]?.after).toBeUndefined();
    });
    expect(showAllButton()).not.toBeInTheDocument();
  });

  it("keeps an explicit after and does not announce the default week", async () => {
    const explicit = "2020-01-01T00:00:00.000Z";
    const events = mockEventLogs();

    renderEvents(`/events?after=${explicit}`, "/events");

    await waitFor(() => expect(events).toHaveBeenCalled());
    expect(events.mock.calls.every((call) => call[0]?.after === explicit)).toBe(true);
    expect(showAllButton()).not.toBeInTheDocument();
    expect(decodeURIComponent(latestSearch)).toContain(explicit);
    expect(decodeURIComponent(latestSearch)).not.toContain(expectedAfter());
  });

  it("lists every event when all_events is set", async () => {
    const events = mockEventLogs();

    renderEvents("/events?all_events=1", "/events");

    await waitFor(() => expect(events).toHaveBeenCalled());
    expect(events.mock.calls.every((call) => call[0]?.after === undefined)).toBe(true);
    expect(showAllButton()).not.toBeInTheDocument();
  });

  it("keeps a sort that is already in the address", async () => {
    const events = mockEventLogs();

    renderEvents("/events?sort=event", "/events");

    await waitFor(() => expect(events.mock.lastCall?.[0]?.orderBy).toEqual(["event"]));
    expect(latestSearch).toContain("sort=event");
    expect(latestSearch).not.toContain("sort=-when");
  });

  it("keeps a saved sort instead of forcing When", async () => {
    localStorage.setItem("events-table-sort", JSON.stringify([{ desc: false, id: "event" }]));
    const events = mockEventLogs();

    renderEvents("/events", "/events");

    await waitFor(() => expect(events.mock.lastCall?.[0]?.orderBy).toEqual(["event"]));
    expect(latestSearch).not.toContain("sort=");
  });
});

describe("nested audit log", () => {
  it("does not apply the browse week", async () => {
    const events = mockEventLogs();

    renderEvents("/dags/my_dag/events", "/dags/:dagId/events");

    await waitFor(() => expect(events).toHaveBeenCalled());
    expect(events.mock.lastCall?.[0]?.after).toBeUndefined();
    expect(events.mock.lastCall?.[0]?.dagId).toBe("my_dag");
    expect(events.mock.lastCall?.[0]?.orderBy).toEqual(["-when"]);
    expect(showAllButton()).not.toBeInTheDocument();
    expect(latestSearch).not.toContain("after=");
    expect(latestSearch).not.toContain("sort=");
  });
});

it("filters a regional audit tab by the pass coordinates, keeping every try and its try filter", async () => {
  const regionId = "11111111-1111-4111-8111-111111111111";
  const task = vi.spyOn(TaskInstanceService, "getMappedTaskInstance");
  const events = vi
    .spyOn(EventLogService, "getEventLogs")
    .mockResolvedValue({ event_logs: [], total_entries: 0 });

  render(
    <BaseWrapper>
      <MemoryRouter
        initialEntries={[
          `/dags/dag/runs/run/tasks/work/events?region_id=${regionId}&region_index=3&map_index=-1&try_number=2`,
        ]}
      >
        <Routes>
          <Route element={<Events />} path="/dags/:dagId/runs/:runId/tasks/:taskId/events" />
        </Routes>
      </MemoryRouter>
    </BaseWrapper>,
  );
  await waitFor(() => expect(events).toHaveBeenCalled());
  expect(task).not.toHaveBeenCalled();
  expect(events.mock.lastCall?.[0]).toMatchObject({
    after: undefined,
    mapIndex: undefined,
    regionId,
    regionIndex: 3,
    taskInstanceId: undefined,
    tryNumber: 2,
  });
});
