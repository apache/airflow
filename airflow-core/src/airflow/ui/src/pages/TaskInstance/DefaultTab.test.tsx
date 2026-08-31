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
import { MemoryRouter, Route, Routes, useLocation } from "react-router-dom";
import { describe, it, expect, vi, beforeEach } from "vitest";

import { useDefaultTaskInstanceTab } from "src/hooks/useUserSettings";

import { DefaultTab } from "./DefaultTab";

vi.mock("src/hooks/useUserSettings", () => ({ useDefaultTaskInstanceTab: vi.fn() }));
vi.mock("./Logs", () => ({ Logs: () => <div>logs-page</div> }));

const setDefaultTab = (value: unknown) =>
  vi
    .mocked(useDefaultTaskInstanceTab)
    .mockReturnValue([value as never, vi.fn(), vi.fn()] as unknown as ReturnType<
      typeof useDefaultTaskInstanceTab
    >);

const LocationEcho = ({ label }: { readonly label: string }) => {
  const { search } = useLocation();

  return (
    <div>
      {label}
      {search}
    </div>
  );
};

const renderAt = (url: string) =>
  render(
    <MemoryRouter initialEntries={[url]}>
      <Routes>
        <Route path="dags/:dagId/runs/:runId/tasks/:taskId">
          <Route element={<DefaultTab />} index />
          <Route element={<div>logs-page</div>} path="logs" />
          <Route element={<LocationEcho label="details-page" />} path="details" />
        </Route>
      </Routes>
    </MemoryRouter>,
  );

describe("DefaultTab", () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it("renders logs on the index route by default", () => {
    setDefaultTab("logs");
    renderAt("/dags/d1/runs/r1/tasks/t1");

    expect(screen.getByText("logs-page")).toBeInTheDocument();
  });

  it("redirects to the configured tab", () => {
    setDefaultTab("details");
    renderAt("/dags/d1/runs/r1/tasks/t1");

    expect(screen.getByText("details-page")).toBeInTheDocument();
  });

  it("preserves the query string when redirecting", () => {
    setDefaultTab("details");
    renderAt("/dags/d1/runs/r1/tasks/t1?foo=bar");

    expect(screen.getByText("details-page?foo=bar")).toBeInTheDocument();
  });

  it("falls back to logs for an unknown tab value", () => {
    setDefaultTab("not-a-tab");
    renderAt("/dags/d1/runs/r1/tasks/t1");

    expect(screen.getByText("logs-page")).toBeInTheDocument();
  });
});
