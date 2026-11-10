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

import { render } from "@testing-library/react";
import { MemoryRouter, Route, Routes } from "react-router-dom";
import { expect, it, vi } from "vitest";

import type * as OpenApiQueries from "openapi/queries";

import type * as Utils from "src/utils";
import { BaseWrapper } from "src/utils/Wrapper";

import { DetailsLayout } from "./DetailsLayout";

const { mockUseDagRunServiceGetDagRun, mockUseGridRuns } = vi.hoisted(() => ({
  mockUseDagRunServiceGetDagRun: vi.fn(() => ({ data: undefined })),
  mockUseGridRuns: vi.fn(() => ({
    data: [{ run_id: "run_2" }, { run_id: "run_1" }, { run_id: "lookahead_run" }],
  })),
}));

vi.mock("@xyflow/react", () => ({
  useReactFlow: () => ({ fitView: vi.fn(), getZoom: () => 1 }),
}));

vi.mock("react-i18next", () => ({
  useTranslation: () => ({
    i18n: { dir: () => "ltr" },
    // eslint-disable-next-line id-length
    t: (key: string) => key,
  }),
}));

vi.mock("react-resizable-panels", () => ({
  Group: ({ children }: PropsWithChildren) => children,
  Panel: ({ children }: PropsWithChildren) => children,
  Separator: ({ children }: PropsWithChildren) => children,
  useDefaultLayout: () => ({ defaultLayout: undefined, onLayoutChanged: vi.fn() }),
  useGroupRef: () => ({ current: null }),
}));

vi.mock("usehooks-ts", () => ({
  useLocalStorage: (_storageKey: string, initialValue: unknown) => [initialValue, vi.fn()],
}));

vi.mock("openapi/queries", async (importOriginal) => ({
  ...(await importOriginal<typeof OpenApiQueries>()),
  useDagRunServiceGetDagRun: mockUseDagRunServiceGetDagRun,
  useDagRunServiceGetDagRuns: () => ({ data: undefined }),
  useDagServiceGetDag: () => ({ data: undefined }),
}));

vi.mock("src/components/DagStatusChip", () => ({ DagStatusChip: () => null }));
vi.mock("src/context/groups", () => ({ GroupsProvider: ({ children }: PropsWithChildren) => children }));
vi.mock("src/hooks/useDagRunsLimit", () => ({ useDagRunsLimit: () => ({ limit: 2, setLimit: vi.fn() }) }));
vi.mock("src/queries/useGridRuns.ts", () => ({ useGridRuns: mockUseGridRuns }));
vi.mock("src/utils", async (importOriginal) => ({
  ...(await importOriginal<typeof Utils>()),
  useAutoRefresh: () => false,
  useContainerWidth: () => 1000,
}));
vi.mock("./DagBreadcrumb", () => ({ DagBreadcrumb: () => null }));
vi.mock("./Gantt/Gantt", () => ({ Gantt: () => null }));
vi.mock("./Graph", () => ({ Graph: () => null }));
vi.mock("./Grid", () => ({ Grid: () => null }));
vi.mock("./Grid/useGridCrosshairHover", () => ({ useGridCrosshairHover: vi.fn() }));
vi.mock("./NavTabs", () => ({ NavTabs: () => null }));
vi.mock("./PanelButtons", () => ({ PanelButtons: () => null }));
vi.mock("./runLimitConfig", () => ({ getEffectiveLimit: (limit: number) => limit }));

it("positions a selected run that is only present as the page look-ahead row", () => {
  render(
    <BaseWrapper>
      <MemoryRouter initialEntries={["/dags/test_dag/runs/lookahead_run"]}>
        <Routes>
          <Route element={<DetailsLayout tabs={[]} />} path="/dags/:dagId/runs/:runId" />
        </Routes>
      </MemoryRouter>
    </BaseWrapper>,
  );

  expect(mockUseGridRuns).toHaveBeenCalledWith(expect.objectContaining({ limit: 3, offset: 0 }), {
    autoRefresh: false,
    enabled: true,
  });
  expect(mockUseDagRunServiceGetDagRun).toHaveBeenCalledWith(
    { dagId: "test_dag", dagRunId: "lookahead_run" },
    undefined,
    { enabled: true },
  );
});
