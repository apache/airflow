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
import { render, screen } from "@testing-library/react";
import { MemoryRouter, Route, Routes } from "react-router-dom";
import { describe, expect, it, vi } from "vitest";

import type * as OpenapiQueries from "openapi/queries";

import { BaseWrapper } from "src/utils/Wrapper";

import { Dag } from "./Dag";

vi.mock("openapi/queries", async (importOriginal) => {
  const actual = await importOriginal<typeof OpenapiQueries>();

  return {
    ...actual,
    useDagServiceGetDagDetails: vi.fn(() => ({ data: undefined, error: null })),
    useDagServiceGetLatestRunInfo: vi.fn(),
  };
});
vi.mock("src/hooks/usePluginTabs", () => ({ usePluginTabs: vi.fn(() => []) }));
vi.mock("src/queries/useRefreshOnNewDagRuns", () => ({ useRefreshOnNewDagRuns: vi.fn() }));
vi.mock("src/layouts/Details/DetailsPanel", () => ({
  DetailsPanel: ({ children }: PropsWithChildren) => children,
}));
vi.mock("./Header", () => ({
  Header: ({ error }: { readonly error?: unknown }) =>
    error === null || error === undefined ? undefined : <div data-testid="header-error" />,
}));
vi.mock("src/utils", async () => {
  const actual = await vi.importActual("src/utils");

  return { ...actual, useAutoRefresh: vi.fn(() => false), useDocumentTitle: vi.fn() };
});

const { useDagServiceGetLatestRunInfo } = await import("openapi/queries");

describe("Dag", () => {
  // The latest-run lookup is about a run, not the Dag, so its failure belongs on the card and the
  // Dag chip stays quiet.
  it("reports a failed latest-run lookup on the card", () => {
    vi.mocked(useDagServiceGetLatestRunInfo).mockReturnValue({
      data: undefined,
      error: new Error("request failed"),
    } as unknown as ReturnType<typeof useDagServiceGetLatestRunInfo>);

    render(
      <BaseWrapper>
        <MemoryRouter initialEntries={["/dags/my_dag"]}>
          <Routes>
            <Route element={<Dag />} path="/dags/:dagId" />
          </Routes>
        </MemoryRouter>
      </BaseWrapper>,
    );

    expect(screen.getByTestId("header-error")).toBeInTheDocument();
  });
});
