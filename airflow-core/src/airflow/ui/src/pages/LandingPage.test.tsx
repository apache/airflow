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
import { describe, it, expect, vi, beforeEach } from "vitest";

import { useDefaultLandingPage } from "src/hooks/useUserSettings";

import { LandingPage } from "./LandingPage";

vi.mock("src/hooks/useUserSettings", () => ({ useDefaultLandingPage: vi.fn() }));
vi.mock("src/pages/Dashboard", () => ({ Dashboard: () => <div>dashboard-page</div> }));

const setLandingPage = (value: unknown) =>
  vi
    .mocked(useDefaultLandingPage)
    .mockReturnValue([value as never, vi.fn(), vi.fn()] as unknown as ReturnType<
      typeof useDefaultLandingPage
    >);

const renderAt = () =>
  render(
    <MemoryRouter initialEntries={["/"]}>
      <Routes>
        <Route element={<LandingPage />} index />
        <Route element={<div>dags-page</div>} path="dags" />
      </Routes>
    </MemoryRouter>,
  );

describe("LandingPage", () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it("renders the dashboard by default", () => {
    setLandingPage("dashboard");
    renderAt();

    expect(screen.getByText("dashboard-page")).toBeInTheDocument();
  });

  it("redirects to the Dags list when configured", () => {
    setLandingPage("dags");
    renderAt();

    expect(screen.getByText("dags-page")).toBeInTheDocument();
  });
});
