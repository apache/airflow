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
import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import dayjs from "dayjs";
import { MemoryRouter, Route, Routes, useLocation, useSearchParams } from "react-router-dom";
import { afterEach, expect, it, vi } from "vitest";

import { EventLogService, TaskInstanceService } from "openapi/requests";

import { TimezoneProvider } from "src/context/timezone";
import { BaseWrapper } from "src/utils/Wrapper";

import { Events } from "./Events";

vi.mock("src/queries/useConfig", () => ({ useConfig: () => false }));
afterEach(() => vi.restoreAllMocks());

const SearchProbe = () => <output data-testid="search">{useLocation().search}</output>;

const ClearAfter = () => {
  const [, setSearchParams] = useSearchParams();

  return (
    <button
      onClick={() =>
        setSearchParams((prev) => {
          const next = new URLSearchParams(prev);

          next.delete("after");

          return next;
        })
      }
      type="button"
    >
      clear
    </button>
  );
};

const renderEvents = (entry: string) =>
  render(
    <BaseWrapper>
      <MemoryRouter initialEntries={[entry]}>
        <TimezoneProvider>
          <Routes>
            <Route
              element={
                <>
                  <Events />
                  <SearchProbe />
                  <ClearAfter />
                </>
              }
              path="/events"
            />
            <Route element={<Events />} path="/dags/:dagId/events" />
          </Routes>
        </TimezoneProvider>
      </MemoryRouter>
    </BaseWrapper>,
  );

const mockEventLogs = () =>
  vi.spyOn(EventLogService, "getEventLogs").mockResolvedValue({ event_logs: [], total_entries: 0 });

it("opens the audit log on the last 7 days and puts the default in the URL", async () => {
  const events = mockEventLogs();

  renderEvents("/events");

  await waitFor(() => expect(screen.getByTestId("search").textContent).toContain("after="));
  const after = new URLSearchParams(screen.getByTestId("search").textContent).get("after");

  expect(dayjs().diff(after, "day", true)).toBeCloseTo(7, 2);
  // The first request already carries the default, so the unfiltered query never runs.
  expect(events.mock.calls.every(([params]) => params?.after === after)).toBe(true);
});

it("shows every event once the default date filter is cleared", async () => {
  const events = mockEventLogs();

  renderEvents("/events");

  await waitFor(() => expect(screen.getByTestId("search").textContent).toContain("after="));
  fireEvent.click(screen.getByRole("button", { name: "clear" }));

  await waitFor(() => expect(events.mock.lastCall?.[0]?.after).toBeUndefined());
  expect(screen.getByTestId("search").textContent).not.toContain("after=");
});

it.each([
  ["/events?after=2026-01-01T00:00:00.000Z", "2026-01-01T00:00:00.000Z", undefined],
  ["/events?before=2026-01-01T00:00:00.000Z", undefined, "2026-01-01T00:00:00.000Z"],
])("keeps the date range from %s instead of the default", async (entry, after, before) => {
  const events = mockEventLogs();

  renderEvents(entry);

  await waitFor(() => expect(events).toHaveBeenCalled());
  expect(events.mock.lastCall?.[0]).toMatchObject({ after, before });
  expect(events.mock.calls.every(([params]) => params?.after === after)).toBe(true);
});

it("does not apply the default date filter on a Dag's audit log tab", async () => {
  const events = mockEventLogs();

  renderEvents("/dags/dag/events");

  await waitFor(() => expect(events).toHaveBeenCalled());
  expect(events.mock.calls.every(([params]) => params?.after === undefined)).toBe(true);
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
    mapIndex: undefined,
    regionId,
    regionIndex: 3,
    taskInstanceId: undefined,
    tryNumber: 2,
  });
});
