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
import { useState, type PropsWithChildren } from "react";

import { act, render } from "@testing-library/react";
import { MemoryRouter, Route, Routes } from "react-router-dom";
import { beforeEach, expect, it, vi } from "vitest";

import type * as OpenApiQueries from "openapi/queries";

import type * as Utils from "src/utils";
import { BaseWrapper } from "src/utils/Wrapper";

import { DetailsLayout } from "./DetailsLayout";

const {
  mockPanelButtons,
  mockSetDefaultDagView,
  mockUseDagRunServiceGetDagRun,
  mockUseDefaultDagView,
  mockUseGridRuns,
} = vi.hoisted(() => ({
  mockPanelButtons: vi.fn(),
  mockSetDefaultDagView: vi.fn(),
  mockUseDagRunServiceGetDagRun: vi.fn(() => ({ data: undefined })),
  mockUseDefaultDagView: vi.fn(() => ["grid", vi.fn()]),
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

const mockLocalStorageStore = new Map<string, unknown>();
const mockSetLocalStorage = vi.fn((key: string, value: unknown) => {
  mockLocalStorageStore.set(key, value);
});

vi.mock("usehooks-ts", () => ({
  useLocalStorage: (storageKey: string, initialValue: unknown) => {
    const [storedValue, setStoredValue] = useState(() =>
      mockLocalStorageStore.has(storageKey) ? mockLocalStorageStore.get(storageKey) : initialValue,
    );
    const setValue = (nextValue: unknown) => {
      const resolved =
        typeof nextValue === "function" ? (nextValue as (prev: unknown) => unknown)(storedValue) : nextValue;

      mockSetLocalStorage(storageKey, resolved);
      mockLocalStorageStore.set(storageKey, resolved);
      setStoredValue(resolved);
    };

    return [storedValue, setValue];
  },
}));

vi.mock("src/hooks/useUserSettings", () => ({
  useDefaultDagView: () => mockUseDefaultDagView(),
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
vi.mock("./PanelButtons", () => ({
  PanelButtons: (props: unknown) => {
    mockPanelButtons(props);

    return null;
  },
}));
vi.mock("./runLimitConfig", () => ({ getEffectiveLimit: (limit: number) => limit }));

beforeEach(() => {
  vi.clearAllMocks();
  mockLocalStorageStore.clear();
  mockUseDefaultDagView.mockReturnValue(["grid", mockSetDefaultDagView]);
});

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

it("falls back to grid when defaultDagView is gantt but no runId is present without mutating setting", () => {
  mockUseDefaultDagView.mockReturnValue(["gantt", mockSetDefaultDagView]);

  const { unmount } = render(
    <BaseWrapper>
      <MemoryRouter initialEntries={["/dags/test_dag"]}>
        <Routes>
          <Route element={<DetailsLayout tabs={[]} />} path="/dags/:dagId" />
        </Routes>
      </MemoryRouter>
    </BaseWrapper>,
  );

  expect(mockPanelButtons).toHaveBeenCalledWith(expect.objectContaining({ dagView: "grid" }));
  expect(mockSetDefaultDagView).not.toHaveBeenCalled();
  expect(mockSetLocalStorage).not.toHaveBeenCalled();
  expect(mockLocalStorageStore.has("dag_view-test_dag")).toBe(false);

  unmount();

  render(
    <BaseWrapper>
      <MemoryRouter initialEntries={["/dags/test_dag/runs/run_1"]}>
        <Routes>
          <Route element={<DetailsLayout tabs={[]} />} path="/dags/:dagId/runs/:runId" />
        </Routes>
      </MemoryRouter>
    </BaseWrapper>,
  );

  expect(mockPanelButtons).toHaveBeenLastCalledWith(expect.objectContaining({ dagView: "gantt" }));
});

it("retains in-page gantt view selection when navigating back to a run after visiting a runless subroute", () => {
  mockUseDefaultDagView.mockReturnValue(["grid", mockSetDefaultDagView]);

  const { unmount } = render(
    <BaseWrapper>
      <MemoryRouter initialEntries={["/dags/test_dag/runs/run_1"]}>
        <Routes>
          <Route element={<DetailsLayout tabs={[]} />} path="/dags/:dagId/runs/:runId" />
        </Routes>
      </MemoryRouter>
    </BaseWrapper>,
  );

  const lastCallProps = mockPanelButtons.mock.calls.at(-1)?.[0] as {
    setDagView: (view: string) => void;
  };

  act(() => {
    lastCallProps.setDagView("gantt");
  });

  expect(mockPanelButtons).toHaveBeenLastCalledWith(expect.objectContaining({ dagView: "gantt" }));

  unmount();

  const { unmount: unmountRunless } = render(
    <BaseWrapper>
      <MemoryRouter initialEntries={["/dags/test_dag"]}>
        <Routes>
          <Route element={<DetailsLayout tabs={[]} />} path="/dags/:dagId" />
        </Routes>
      </MemoryRouter>
    </BaseWrapper>,
  );

  expect(mockPanelButtons).toHaveBeenLastCalledWith(expect.objectContaining({ dagView: "grid" }));
  expect(mockLocalStorageStore.get("dag_view-test_dag")).toBe("gantt");

  unmountRunless();

  render(
    <BaseWrapper>
      <MemoryRouter initialEntries={["/dags/test_dag/runs/run_2"]}>
        <Routes>
          <Route element={<DetailsLayout tabs={[]} />} path="/dags/:dagId/runs/:runId" />
        </Routes>
      </MemoryRouter>
    </BaseWrapper>,
  );

  expect(mockPanelButtons).toHaveBeenLastCalledWith(expect.objectContaining({ dagView: "gantt" }));
});

it("uses gantt view when defaultDagView is gantt and runId is present", () => {
  mockUseDefaultDagView.mockReturnValue(["gantt", mockSetDefaultDagView]);

  render(
    <BaseWrapper>
      <MemoryRouter initialEntries={["/dags/test_dag/runs/run_1"]}>
        <Routes>
          <Route element={<DetailsLayout tabs={[]} />} path="/dags/:dagId/runs/:runId" />
        </Routes>
      </MemoryRouter>
    </BaseWrapper>,
  );

  expect(mockPanelButtons).toHaveBeenCalledWith(expect.objectContaining({ dagView: "gantt" }));
  expect(mockSetDefaultDagView).not.toHaveBeenCalled();
});

it("allows toggling view in page without mutating the defaultDagView setting", () => {
  mockUseDefaultDagView.mockReturnValue(["grid", mockSetDefaultDagView]);

  render(
    <BaseWrapper>
      <MemoryRouter initialEntries={["/dags/test_dag"]}>
        <Routes>
          <Route element={<DetailsLayout tabs={[]} />} path="/dags/:dagId" />
        </Routes>
      </MemoryRouter>
    </BaseWrapper>,
  );

  expect(mockPanelButtons).toHaveBeenCalledWith(expect.objectContaining({ dagView: "grid" }));

  const lastCallProps = mockPanelButtons.mock.calls.at(-1)?.[0] as {
    setDagView: (view: string) => void;
  };

  act(() => {
    lastCallProps.setDagView("graph");
  });

  expect(mockPanelButtons).toHaveBeenLastCalledWith(expect.objectContaining({ dagView: "graph" }));
  expect(mockSetDefaultDagView).not.toHaveBeenCalled();
});

it("preserves active view selection across sub-routes within the DAG", () => {
  mockUseDefaultDagView.mockReturnValue(["grid", mockSetDefaultDagView]);

  const { unmount } = render(
    <BaseWrapper>
      <MemoryRouter initialEntries={["/dags/test_dag"]}>
        <Routes>
          <Route element={<DetailsLayout tabs={[]} />} path="/dags/:dagId" />
        </Routes>
      </MemoryRouter>
    </BaseWrapper>,
  );

  const lastCallProps = mockPanelButtons.mock.calls.at(-1)?.[0] as {
    setDagView: (view: string) => void;
  };

  act(() => {
    lastCallProps.setDagView("graph");
  });

  unmount();

  render(
    <BaseWrapper>
      <MemoryRouter initialEntries={["/dags/test_dag/tasks/test_task"]}>
        <Routes>
          <Route element={<DetailsLayout tabs={[]} />} path="/dags/:dagId/tasks/:taskId" />
        </Routes>
      </MemoryRouter>
    </BaseWrapper>,
  );

  expect(mockPanelButtons).toHaveBeenLastCalledWith(expect.objectContaining({ dagView: "graph" }));
  expect(mockSetDefaultDagView).not.toHaveBeenCalled();
});
