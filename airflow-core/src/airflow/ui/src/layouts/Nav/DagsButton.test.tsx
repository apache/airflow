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
import { MemoryRouter } from "react-router-dom";
import { describe, expect, it } from "vitest";

import { BaseWrapper, Wrapper } from "src/utils/Wrapper";

import { DagsButton } from "./DagsButton";

const getLink = () => screen.getByRole("link", { name: "nav.dags" });

const wrapperAt = (path: string) => {
  const wrapper = ({ children }: PropsWithChildren) => (
    <BaseWrapper>
      <MemoryRouter initialEntries={[path]}>{children}</MemoryRouter>
    </BaseWrapper>
  );

  return wrapper;
};

describe("DagsButton", () => {
  it("is a link to the Dags page", () => {
    render(<DagsButton authorizedMenuItems={["Dags"]} />, { wrapper: Wrapper });

    expect(getLink()).toHaveAttribute("href", "/dags");
  });

  it.each(["/dags", "/dags/example_dag/runs"])("is highlighted on %s", (path) => {
    render(<DagsButton authorizedMenuItems={["Dags"]} />, { wrapper: wrapperAt(path) });

    expect(getLink()).toHaveAttribute("aria-current", "page");
  });

  it.each(["/dag_runs", "/task_instances"])("is not highlighted on %s", (path) => {
    render(<DagsButton authorizedMenuItems={["Dags"]} />, { wrapper: wrapperAt(path) });

    expect(getLink()).not.toHaveAttribute("aria-current");
  });

  it("does not open a menu on hover", () => {
    render(<DagsButton authorizedMenuItems={["Dags"]} />, { wrapper: Wrapper });

    fireEvent.pointerEnter(getLink(), { pointerType: "mouse" });

    expect(screen.queryByRole("menuitem")).toBeNull();
  });

  it("is not a link when Dags is not authorized", () => {
    render(<DagsButton authorizedMenuItems={[]} />, { wrapper: Wrapper });

    expect(screen.queryByRole("link", { name: "nav.dags" })).toBeNull();
    expect(screen.getByRole("button", { name: "nav.dags" })).toBeDisabled();
  });
});
