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
import { render, waitFor } from "@testing-library/react";
import type { ReactNode } from "react";
import { MemoryRouter, Route, Routes } from "react-router-dom";
import { describe, expect, it, vi } from "vitest";

import { useDocumentTitle, DocumentTitleProvider } from "src/hooks/useDocumentTitle";
import { BaseWrapper } from "src/utils/Wrapper";

import { Dag } from "./Dag";

const mockDagWithDisplayName = vi.hoisted(() => ({
  active_runs_count: 0,
  allowed_run_types: [],
  dag_display_name: "TaskFlow Tutorial",
  dag_id: "display_name_dag",
  is_paused: false,
  is_stale: false,
  timetable_summary: null,
}));

vi.mock("openapi/queries", () => ({
  useConfigServiceGetConfigs: () => ({ data: { instance_name: "Airflow" } }),
  useDagRunServiceGetDagRuns: () => ({ data: { dag_runs: [] } }),
  useDagServiceGetDagDetails: () => ({ data: mockDagWithDisplayName, isLoading: false }),
  useDagServiceGetLatestRunInfo: () => ({ data: null, isLoading: false }),
}));
vi.mock("react-i18next", () => ({
  useTranslation: () => Object.fromEntries([["t", (translationKey: string) => translationKey]]),
}));
vi.mock("src/hooks/usePluginTabs", () => ({ usePluginTabs: () => [] }));
vi.mock("src/hooks/useRequiredActionTabs", () => ({
  useRequiredActionTabs: (_params: unknown, tabs: unknown) => ({ tabs }),
}));
vi.mock("src/layouts/Details/DetailsLayout", () => ({
  DetailsLayout: ({ children }: { readonly children?: ReactNode }) => children,
}));
vi.mock("./Header", () => ({ Header: () => null }));

const renderDag = (initialEntry: string) =>
  render(
    <BaseWrapper>
      <DocumentTitleProvider>
        <MemoryRouter initialEntries={[initialEntry]}>
          <Routes>
            <Route element={<Dag />} path="/dags/:dagId" />
          </Routes>
        </MemoryRouter>
      </DocumentTitleProvider>
    </BaseWrapper>,
  );

const RouteWithoutPageTitle = () => {
  useDocumentTitle(undefined);

  return null;
};

const renderTitleRoutes = (initialEntry: string) =>
  render(
    <BaseWrapper>
      <DocumentTitleProvider>
        <MemoryRouter initialEntries={[initialEntry]}>
          <Routes>
            <Route element={<Dag />} path="/dags/:dagId" />
            <Route element={<RouteWithoutPageTitle />} path="/" />
          </Routes>
        </MemoryRouter>
      </DocumentTitleProvider>
    </BaseWrapper>,
  );

describe("Dag", () => {
  it("updates the browser title with the dag display name and instance name", async () => {
    renderDag("/dags/display_name_dag");

    await waitFor(() => expect(document.title).toBe("TaskFlow Tutorial - Airflow"));
  });

  it("restores the instance name when leaving the dag route", async () => {
    const { unmount } = renderTitleRoutes("/dags/display_name_dag");

    await waitFor(() => expect(document.title).toBe("TaskFlow Tutorial - Airflow"));
    unmount();

    renderTitleRoutes("/");

    await waitFor(() => expect(document.title).toBe("Airflow"));
  });
});
