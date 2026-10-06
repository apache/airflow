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
import { render, screen } from "@testing-library/react";
import { MemoryRouter, Route, Routes } from "react-router-dom";
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
  DetailsLayout: ({ tabs }: PropsWithChildren<{ readonly tabs: Array<NavTab> }>) => <NavTabs tabs={tabs} />,
}));
vi.mock("src/utils", async () => {
  const actual = await vi.importActual("src/utils");

  return { ...actual, useAutoRefresh: vi.fn(() => false), useDocumentTitle: vi.fn() };
});

const { useDeadlinesServiceGetDeadlines } = await import("openapi/queries");

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
