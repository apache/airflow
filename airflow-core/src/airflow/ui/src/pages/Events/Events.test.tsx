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
import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import { MemoryRouter, Route, Routes } from "react-router-dom";
import { afterEach, expect, it, vi } from "vitest";

import { EventLogService, TaskInstanceService } from "openapi/requests";

import { BaseWrapper } from "src/utils/Wrapper";

import { Events } from "./Events";

vi.mock("src/queries/useConfig", () => ({ useConfig: () => false }));
afterEach(() => vi.restoreAllMocks());

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

it("pages the audit log by cursor instead of counting rows", async () => {
  const events = vi
    .spyOn(EventLogService, "getEventLogs")
    .mockResolvedValue({ event_logs: [], next_cursor: "page-2", previous_cursor: null });

  render(
    <BaseWrapper>
      <MemoryRouter initialEntries={["/events"]}>
        <Routes>
          <Route element={<Events />} path="/events" />
        </Routes>
      </MemoryRouter>
    </BaseWrapper>,
  );

  await waitFor(() => expect(events).toHaveBeenCalled());
  expect(events.mock.lastCall?.[0]).toMatchObject({ cursor: "" });
  expect(events.mock.lastCall?.[0]?.offset).toBeUndefined();

  const next = await screen.findByTestId("next");

  expect(screen.getByTestId("prev")).toBeDisabled();
  fireEvent.click(next);
  await waitFor(() => expect(events.mock.lastCall?.[0]).toMatchObject({ cursor: "page-2" }));
});

it("names the audit log date filter after the When column it filters", async () => {
  vi.spyOn(EventLogService, "getEventLogs").mockResolvedValue({ event_logs: [] });

  render(
    <BaseWrapper>
      <MemoryRouter initialEntries={["/events"]}>
        <Routes>
          <Route element={<Events />} path="/events" />
        </Routes>
      </MemoryRouter>
    </BaseWrapper>,
  );

  fireEvent.click(await screen.findByTestId("add-filter-button"));

  expect(await screen.findByTestId("add-filter-event_date_range")).toHaveTextContent(
    "browse:auditLog.columns.when",
  );
});
