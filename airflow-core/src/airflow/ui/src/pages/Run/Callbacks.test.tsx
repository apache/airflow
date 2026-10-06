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
import { render, screen } from "@testing-library/react";
import { MemoryRouter, Route, Routes } from "react-router-dom";
import { describe, expect, it, vi } from "vitest";

import type * as OpenapiQueries from "openapi/queries";
import type { DeadlineResponse } from "openapi/requests/types.gen";

import { TimezoneProvider } from "src/context/timezone";
import i18n from "src/i18n/config";
import { BaseWrapper } from "src/utils/Wrapper";

import { Callbacks } from "./Callbacks";

vi.mock("openapi/queries", async (importOriginal) => {
  const actual = await importOriginal<typeof OpenapiQueries>();

  return { ...actual, useDeadlinesServiceGetDeadlines: vi.fn() };
});

const { useDeadlinesServiceGetDeadlines } = await import("openapi/queries");

const deadlines: Array<DeadlineResponse> = [
  {
    alert_name: "sla_alert",
    callback_id: "0199a8e0-0000-7000-8000-000000000001",
    callback_path: "dags.notify.on_miss",
    callback_state: "success",
    callback_type: "executor",
    created_at: "2026-10-01T00:00:00Z",
    dag_id: "my_dag",
    dag_run_id: "run_1",
    deadline_time: "2026-10-01T00:00:05Z",
    id: "0199a8e0-0000-7000-8000-00000000000a",
    missed: true,
  },
  {
    alert_name: null,
    callback_id: "0199a8e0-0000-7000-8000-000000000002",
    callback_path: "dags.notify.async_on_miss",
    callback_state: "pending",
    callback_type: "triggerer",
    created_at: "2026-10-01T00:00:00Z",
    dag_id: "my_dag",
    dag_run_id: "run_1",
    deadline_time: "2026-10-01T00:00:10Z",
    id: "0199a8e0-0000-7000-8000-00000000000b",
    missed: true,
  },
];

describe("Callbacks", () => {
  it("lists the run's callbacks with a link to each callback's logs", () => {
    vi.mocked(useDeadlinesServiceGetDeadlines).mockReturnValue({
      data: { deadlines, total_entries: deadlines.length },
      error: null,
      isFetching: false,
      isLoading: false,
    } as unknown as ReturnType<typeof useDeadlinesServiceGetDeadlines>);

    render(
      <BaseWrapper>
        <MemoryRouter initialEntries={["/dags/my_dag/runs/run_1/callbacks"]}>
          <TimezoneProvider>
            <Routes>
              <Route element={<Callbacks />} path="/dags/:dagId/runs/:runId/callbacks" />
            </Routes>
          </TimezoneProvider>
        </MemoryRouter>
      </BaseWrapper>,
    );

    expect(vi.mocked(useDeadlinesServiceGetDeadlines).mock.lastCall?.[0]).toMatchObject({
      dagId: "my_dag",
      dagRunId: "run_1",
    });
    expect(screen.getByText("dags.notify.on_miss")).toBeInTheDocument();
    expect(screen.getByText("sla_alert")).toBeInTheDocument();
    expect(screen.getByText(i18n.t("common:states.success"))).toBeInTheDocument();
    expect(screen.getByText(i18n.t("common:states.pending"))).toBeInTheDocument();

    const logLinks = screen.getAllByRole("link", { name: i18n.t("dag:tabs.logs") });

    expect(logLinks.map((link) => link.getAttribute("href"))).toEqual(
      deadlines.map(({ callback_id: id }) => `/dags/my_dag/runs/run_1/callbacks/${id}/logs`),
    );
  });
});
