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
import { MemoryRouter } from "react-router-dom";
import { describe, expect, it } from "vitest";

import { BaseWrapper, Wrapper } from "src/utils/Wrapper";

import { RunsButton } from "./RunsButton";

const getLink = () => screen.getByRole("link", { name: "runs" });

const wrapperAt = (path: string) => {
  const wrapper = ({ children }: PropsWithChildren) => (
    <BaseWrapper>
      <MemoryRouter initialEntries={[path]}>{children}</MemoryRouter>
    </BaseWrapper>
  );

  return wrapper;
};

describe("RunsButton", () => {
  it("is a link to the Dag runs page", () => {
    render(<RunsButton authorizedMenuItems={["Dags"]} />, { wrapper: Wrapper });

    expect(getLink()).toHaveAttribute("href", "/dag_runs");
  });

  it("is highlighted on /dag_runs", () => {
    render(<RunsButton authorizedMenuItems={["Dags"]} />, { wrapper: wrapperAt("/dag_runs") });

    expect(getLink()).toHaveAttribute("aria-current", "page");
  });

  it.each(["/dags", "/task_instances"])("is not highlighted on %s", (path) => {
    render(<RunsButton authorizedMenuItems={["Dags"]} />, { wrapper: wrapperAt(path) });

    expect(getLink()).not.toHaveAttribute("aria-current");
  });

  // Dag runs have no menu item of their own, so the Dags permission is what gates them.
  it("is not a link when Dags is not authorized", () => {
    render(<RunsButton authorizedMenuItems={["Jobs"]} />, { wrapper: Wrapper });

    expect(screen.queryByRole("link", { name: "runs" })).toBeNull();
    expect(screen.getByRole("button", { name: "runs" })).toBeDisabled();
  });
});
