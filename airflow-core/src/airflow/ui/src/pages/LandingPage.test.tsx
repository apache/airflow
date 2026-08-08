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
import { Route, Routes } from "react-router-dom";
import { describe, expect, it, vi } from "vitest";

import { Wrapper } from "src/utils/Wrapper";

import { LandingPage } from "./LandingPage";

let mockLandingPage: string | undefined;

vi.mock("src/queries/useConfig", () => ({
  useConfig: () => mockLandingPage,
}));

vi.mock("src/pages/Dashboard", () => ({
  Dashboard: () => <div>dashboard page</div>,
}));

const renderLandingPage = () =>
  render(
    <Routes>
      <Route element={<LandingPage />} index />
      <Route element={<div>dags page</div>} path="/dags" />
    </Routes>,
    { wrapper: Wrapper },
  );

describe("LandingPage", () => {
  it("renders the dashboard by default", () => {
    mockLandingPage = "home";
    renderLandingPage();
    expect(screen.getByText("dashboard page")).toBeInTheDocument();
  });

  it("redirects to the dag list when configured", () => {
    mockLandingPage = "dags";
    renderLandingPage();
    expect(screen.getByText("dags page")).toBeInTheDocument();
  });
});
