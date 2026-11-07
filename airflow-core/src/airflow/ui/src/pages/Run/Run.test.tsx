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
import { fireEvent, render, screen } from "@testing-library/react";
import { Link, MemoryRouter, Route, Routes } from "react-router-dom";
import { describe, expect, it, vi } from "vitest";

import type * as OpenapiQueries from "openapi/queries";

import { NavTabs, type NavTab } from "src/layouts/Details/NavTabs";

import i18n from "src/i18n/config";
import { BaseWrapper } from "src/utils/Wrapper";

import { Run } from "./Run";

vi.mock("openapi/queries", async (importOriginal) => {
  const actual = await importOriginal<typeof OpenapiQueries>();

  return {
    ...actual,
    useDagRunServiceGetDagRun: vi.fn(() => ({ data: undefined, error: null, isLoading: false })),
    useDeadlinesServiceGetDeadlines: vi.fn(),
  };
});
vi.mock("src/hooks/usePluginTabs", () => ({ usePluginTabs: vi.fn(() => []) }));
vi.mock("src/layouts/Details/DetailsLayout", () => ({
  DetailsLayout: ({
    children,
    error,
    tabs,
  }: PropsWithChildren<{ readonly error?: unknown; readonly tabs: Array<NavTab> }>) => (
    <>
      {children}
      {error === null || error === undefined ? undefined : <div data-testid="chip-error" />}
      <NavTabs tabs={tabs} />
    </>
  ),
}));
vi.mock("src/utils", async () => {
  const actual = await vi.importActual("src/utils");

  return { ...actual, useAutoRefresh: vi.fn(() => false), useDocumentTitle: vi.fn() };
});

const { useDagRunServiceGetDagRun, useDeadlinesServiceGetDeadlines } = await import("openapi/queries");

describe("Run", () => {
  it.each([
    { tabCount: 0, totalEntries: 0 },
    { tabCount: 1, totalEntries: 1 },
  ])(
    "shows the Callbacks tab only when the run has callbacks ($totalEntries)",
    ({ tabCount, totalEntries }) => {
      vi.mocked(useDeadlinesServiceGetDeadlines).mockReturnValue({
        data: { deadlines: [], total_entries: totalEntries },
      } as unknown as ReturnType<typeof useDeadlinesServiceGetDeadlines>);

      render(
        <BaseWrapper>
          <MemoryRouter initialEntries={["/dags/my_dag/runs/run_1"]}>
            <Routes>
              <Route element={<Run />} path="/dags/:dagId/runs/:runId" />
            </Routes>
          </MemoryRouter>
        </BaseWrapper>,
      );

      expect(screen.queryAllByTitle(i18n.t("dag:tabs.callbacks"))).toHaveLength(tabCount);
      expect(screen.getByTitle(i18n.t("dag:tabs.assetEvents"))).toBeInTheDocument();
    },
  );
});

const renderRun = () => {
  vi.mocked(useDeadlinesServiceGetDeadlines).mockReturnValue({
    data: { deadlines: [], total_entries: 0 },
  } as unknown as ReturnType<typeof useDeadlinesServiceGetDeadlines>);

  render(
    <BaseWrapper>
      <MemoryRouter initialEntries={["/dags/my_dag/runs/run_1"]}>
        <Routes>
          <Route element={<Run />} path="/dags/:dagId/runs/:runId" />
        </Routes>
      </MemoryRouter>
    </BaseWrapper>,
  );
};

describe("Run header", () => {
  // Without this the card is absent while loading and appears beneath the tabs when the run
  // arrives, pushing everything below it down.
  it("holds the card's space while the run loads", () => {
    vi.mocked(useDagRunServiceGetDagRun).mockReturnValue({
      data: undefined,
      error: null,
      isLoading: true,
    } as unknown as ReturnType<typeof useDagRunServiceGetDagRun>);

    renderRun();

    expect(screen.getByTestId("header-card-skeleton")).toBeVisible();
    expect(screen.queryByTestId("header-card")).not.toBeInTheDocument();
  });

  it("says why the run could not be loaded, keeping the id from the URL", () => {
    vi.mocked(useDagRunServiceGetDagRun).mockReturnValue({
      data: undefined,
      error: new Error("request failed"),
      isLoading: false,
    } as unknown as ReturnType<typeof useDagRunServiceGetDagRun>);

    renderRun();

    expect(screen.getByTestId("error-alert")).toHaveTextContent("request failed");
    expect(screen.getByTestId("header-card")).toHaveTextContent("run_1");
    // The run's card states the failure; the Dag chip is about the Dag, so it stays quiet.
    expect(screen.queryByTestId("chip-error")).not.toBeInTheDocument();
  });

  // The route element stays mounted when only :runId changes, so a failure held for one run must
  // not be shown against the next while that one loads.
  it("drops the previous run's failure when moving to another run", () => {
    vi.mocked(useDagRunServiceGetDagRun).mockImplementation(
      ({ dagRunId }) =>
        (dagRunId === "run_1"
          ? { data: undefined, error: new Error("run_1 not found"), isLoading: false }
          : { data: undefined, error: null, isLoading: true }) as unknown as ReturnType<
          typeof useDagRunServiceGetDagRun
        >,
    );
    vi.mocked(useDeadlinesServiceGetDeadlines).mockReturnValue({
      data: { deadlines: [], total_entries: 0 },
    } as unknown as ReturnType<typeof useDeadlinesServiceGetDeadlines>);

    render(
      <BaseWrapper>
        <MemoryRouter initialEntries={["/dags/my_dag/runs/run_1"]}>
          <Link to="/dags/my_dag/runs/run_2">next run</Link>
          <Routes>
            <Route element={<Run />} path="/dags/:dagId/runs/:runId" />
          </Routes>
        </MemoryRouter>
      </BaseWrapper>,
    );

    expect(screen.getByTestId("error-alert")).toHaveTextContent("run_1 not found");

    fireEvent.click(screen.getByText("next run"));

    expect(screen.queryByTestId("error-alert")).not.toBeInTheDocument();
    expect(screen.getByTestId("header-card-skeleton")).toBeVisible();
  });

  it("leaves the tabs reachable when the run fails, since its data may outlive it", () => {
    vi.mocked(useDagRunServiceGetDagRun).mockReturnValue({
      data: undefined,
      error: new Error("request failed"),
      isLoading: false,
    } as unknown as ReturnType<typeof useDagRunServiceGetDagRun>);

    renderRun();

    expect(screen.getByTitle(i18n.t("dag:tabs.assetEvents"))).toBeInTheDocument();
  });

  // A failure outranks the skeleton, so a fetch still in flight cannot put the skeleton back over
  // what the header already said. useStickyError.test.ts covers holding that failure across the
  // refetch that clears it.
  it("keeps the failure visible while a retry is in flight", () => {
    vi.mocked(useDagRunServiceGetDagRun).mockReturnValue({
      data: undefined,
      error: new Error("request failed"),
      isLoading: true,
    } as unknown as ReturnType<typeof useDagRunServiceGetDagRun>);

    renderRun();

    expect(screen.getByTestId("error-alert")).toHaveTextContent("request failed");
    expect(screen.queryByTestId("header-card-skeleton")).not.toBeInTheDocument();
  });
});
