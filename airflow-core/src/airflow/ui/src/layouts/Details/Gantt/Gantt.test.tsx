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
import type { PropsWithChildren } from "react";

import "@testing-library/jest-dom";
import { cleanup, render } from "@testing-library/react";
import { MemoryRouter, Route, Routes } from "react-router-dom";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import type * as OpenapiQueries from "openapi/queries";

import { TimezoneProvider } from "src/context/timezone";
import { BaseWrapper } from "src/utils/Wrapper";

import { Gantt } from "./Gantt";

const { mockUseGanttServiceGetGanttData } = vi.hoisted(() => ({
  mockUseGanttServiceGetGanttData: vi.fn(() => ({ data: undefined, isLoading: false })),
}));

vi.mock("openapi/queries", async (importOriginal) => ({
  ...(await importOriginal<typeof OpenapiQueries>()),
  useGanttServiceGetGanttData: mockUseGanttServiceGetGanttData,
}));

vi.mock("src/queries/useGridRuns", () => ({
  useGridRuns: () => ({
    data: [
      {
        end_date: "2024-03-14T10:10:00Z",
        run_after: "2024-03-14T10:00:00Z",
        run_id: "run_1",
        start_date: "2024-03-14T10:00:00Z",
        state: "success",
      },
    ],
    isLoading: false,
  }),
}));

vi.mock("src/queries/useGridStructure", () => ({
  useGridStructure: () => ({ data: [], isLoading: false }),
}));

vi.mock("src/queries/useGridTISummaries", () => ({
  useGridTiSummariesStream: () => ({
    summariesByRunId: new Map([["run_1", { task_instances: [] }]]),
  }),
}));

vi.mock("src/context/groups", () => ({
  useGroups: () => ({ openGroupIds: [], toggleGroupId: vi.fn() }),
}));

const createWrapper =
  (initialEntries: Array<string>) =>
  ({ children }: PropsWithChildren) => (
    <BaseWrapper>
      <MemoryRouter initialEntries={initialEntries}>
        <TimezoneProvider>
          <Routes>
            <Route element={children} path="/dags/:dagId/runs/:runId" />
          </Routes>
        </TimezoneProvider>
      </MemoryRouter>
    </BaseWrapper>
  );

beforeEach(() => {
  mockUseGanttServiceGetGanttData.mockClear();
});

afterEach(cleanup);

describe("Gantt time range filter params", () => {
  it("passes the URL date window to the backend query", () => {
    render(<Gantt limit={10} />, {
      wrapper: createWrapper([
        "/dags/test_dag/runs/run_1" +
          "?start_date_gte=2024-03-14T10:00:00Z&start_date_lte=2024-03-14T10:05:00Z" +
          "&end_date_gte=2024-03-14T10:01:00Z&end_date_lte=2024-03-14T10:10:00Z",
      ]),
    });

    expect(mockUseGanttServiceGetGanttData).toHaveBeenCalledWith(
      {
        dagId: "test_dag",
        endDateGte: "2024-03-14T10:01:00Z",
        endDateLte: "2024-03-14T10:10:00Z",
        runId: "run_1",
        startDateGte: "2024-03-14T10:00:00Z",
        startDateLte: "2024-03-14T10:05:00Z",
      },
      undefined,
      expect.anything(),
    );
  });

  it("requests the full run when no date window is set", () => {
    render(<Gantt limit={10} />, {
      wrapper: createWrapper(["/dags/test_dag/runs/run_1"]),
    });

    expect(mockUseGanttServiceGetGanttData).toHaveBeenCalledWith(
      {
        dagId: "test_dag",
        endDateGte: undefined,
        endDateLte: undefined,
        runId: "run_1",
        startDateGte: undefined,
        startDateLte: undefined,
      },
      undefined,
      expect.anything(),
    );
  });
});
