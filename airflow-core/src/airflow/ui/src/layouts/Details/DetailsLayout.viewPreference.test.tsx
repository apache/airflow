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

import "@testing-library/jest-dom/vitest";
import { act, fireEvent, render, screen, within } from "@testing-library/react";
import { MemoryRouter, Route, Routes, useNavigate } from "react-router-dom";
import { beforeEach, describe, expect, it, vi } from "vitest";

import type { DagView } from "src/constants/dagView";
import { DEFAULT_DAG_VIEW_KEY } from "src/constants/localStorage";
import { BaseWrapper } from "src/utils/Wrapper";

import { DetailsLayout } from "./DetailsLayout";

vi.mock("@xyflow/react", () => ({ useReactFlow: () => ({ fitView: vi.fn(), getZoom: () => 1 }) }));
vi.mock("react-i18next", () => ({
  useTranslation: () => ({
    i18n: { dir: () => "ltr", language: "en" },
    // eslint-disable-next-line id-length
    t: (key: string) => key,
  }),
}));
vi.mock("src/utils", () => ({
  formatNumber: (value: number) => String(value),
  useAutoRefresh: () => false,
  useContainerWidth: () => 1200,
}));
vi.mock("src/system-components", () => ({
  IconButton: () => null,
  ProgressBar: () => null,
  Toaster: () => null,
}));
vi.mock("openapi/queries", () => ({
  useDagRunServiceGetDagRun: () => ({ data: undefined }),
  useDagRunServiceGetDagRuns: () => ({ data: undefined }),
  useDagServiceGetDag: () => ({ data: undefined }),
  useDagWarningServiceListDagWarnings: () => ({ data: undefined }),
}));
vi.mock("src/queries/useGridRuns.ts", () => ({ useGridRuns: () => ({ data: undefined }) }));
vi.mock("src/components/Banner/BackfillBanner", () => ({ default: () => null }));
vi.mock("src/components/Banner/DrainingBanner", () => ({ default: () => null }));
vi.mock("src/components/DAGWarningsModal", () => ({
  countDagWarnings: () => 0,
  DAGWarningsModal: () => null,
}));
vi.mock("src/components/TogglePause", () => ({ TogglePause: () => null }));
vi.mock("src/components/TriggerDag/TriggerDAGButton", () => ({ TriggerDAGButton: () => null }));
vi.mock("src/context/groups", () => ({ GroupsProvider: ({ children }: PropsWithChildren) => children }));
vi.mock("./DagBreadcrumb", () => ({ DagBreadcrumb: () => null }));
vi.mock("./NavTabs", () => ({ NavTabs: () => null }));
vi.mock("./Graph", () => ({ Graph: () => <div data-testid="graph" /> }));
vi.mock("./Grid", () => ({ Grid: () => <div data-testid="grid" /> }));
vi.mock("./Gantt/Gantt", () => ({ Gantt: () => <div data-testid="gantt" /> }));
vi.mock("./PanelButtons", () => ({
  PanelButtons: ({
    dagView,
    setDagView,
  }: {
    readonly dagView: DagView;
    readonly setDagView: (view: DagView) => void;
  }) => (
    <button data-testid="view-toggle" onClick={() => setDagView("gantt")} type="button">
      {dagView}
    </button>
  ),
}));

const NavigateToRun = () => {
  const navigate = useNavigate();

  return (
    <button onClick={() => void navigate("/dags/example/runs/run-1")} type="button">
      Open run
    </button>
  );
};

const Page = ({ path }: { readonly path: string }) => (
  <BaseWrapper>
    <MemoryRouter initialEntries={[path]}>
      <NavigateToRun />
      <Routes>
        <Route element={<DetailsLayout tabs={[]} />} path="/dags/:dagId" />
        <Route element={<DetailsLayout tabs={[]} />} path="/dags/:dagId/runs/:runId" />
      </Routes>
    </MemoryRouter>
  </BaseWrapper>
);

describe("DetailsLayout view preference", () => {
  beforeEach(() => localStorage.clear());

  it("displays Grid on overview without discarding a saved Gantt preference", () => {
    localStorage.setItem(DEFAULT_DAG_VIEW_KEY, JSON.stringify("gantt"));
    render(<Page path="/dags/example" />);

    expect(screen.getByTestId("view-toggle")).toHaveTextContent("grid");
    expect(screen.queryByTestId("gantt")).not.toBeInTheDocument();
    expect(localStorage.getItem(DEFAULT_DAG_VIEW_KEY)).toBe(JSON.stringify("gantt"));
  });

  it("does not overwrite Gantt received from another browser tab", () => {
    render(<Page path="/dags/example" />);

    act(() => {
      localStorage.setItem(DEFAULT_DAG_VIEW_KEY, JSON.stringify("gantt"));
      globalThis.dispatchEvent(new StorageEvent("storage", { key: DEFAULT_DAG_VIEW_KEY }));
    });

    expect(localStorage.getItem(DEFAULT_DAG_VIEW_KEY)).toBe(JSON.stringify("gantt"));
    expect(screen.getByTestId("view-toggle")).toHaveTextContent("grid");
  });

  it("keeps the run in Gantt when an overview is also mounted", () => {
    render(
      <>
        <section data-testid="overview">
          <Page path="/dags/example" />
        </section>
        <section data-testid="run">
          <Page path="/dags/example/runs/run-1" />
        </section>
      </>,
    );
    const run = within(screen.getByTestId("run"));

    fireEvent.click(run.getByTestId("view-toggle"));

    expect(run.getByTestId("view-toggle")).toHaveTextContent("gantt");
    expect(run.getByTestId("gantt")).toBeInTheDocument();
    expect(within(screen.getByTestId("overview")).getByTestId("view-toggle")).toHaveTextContent("grid");
    expect(localStorage.getItem(DEFAULT_DAG_VIEW_KEY)).toBe(JSON.stringify("gantt"));
  });

  it("restores Gantt when opening a run from overview", () => {
    localStorage.setItem(DEFAULT_DAG_VIEW_KEY, JSON.stringify("gantt"));
    render(<Page path="/dags/example" />);

    fireEvent.click(screen.getByRole("button", { name: "Open run" }));

    expect(screen.getByTestId("view-toggle")).toHaveTextContent("gantt");
    expect(screen.getByTestId("gantt")).toBeInTheDocument();
  });
});
