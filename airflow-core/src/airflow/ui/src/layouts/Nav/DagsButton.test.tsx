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
import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import { MemoryRouter } from "react-router-dom";
import { describe, expect, it } from "vitest";

import { BaseWrapper, Wrapper } from "src/utils/Wrapper";

import { DagsButton } from "./DagsButton";

const getTrigger = () => screen.getByRole("button", { name: "nav.dags" });

const wrapperAt = (path: string) => {
  const wrapper = ({ children }: PropsWithChildren) => (
    <BaseWrapper>
      <MemoryRouter initialEntries={[path]}>{children}</MemoryRouter>
    </BaseWrapper>
  );

  return wrapper;
};

describe("DagsButton", () => {
  it("opens the menu on click instead of navigating", async () => {
    render(<DagsButton authorizedMenuItems={["Dags"]} />, { wrapper: Wrapper });

    expect(getTrigger()).not.toHaveAttribute("href");

    fireEvent.click(getTrigger());

    await waitFor(() => {
      expect(screen.getAllByRole("menuitem")).toHaveLength(3);
    });
  });

  it.each(["/dags", "/dags/example_dag/runs", "/dag_runs", "/task_instances"])(
    "is highlighted on %s",
    (path) => {
      render(<DagsButton authorizedMenuItems={["Dags"]} />, { wrapper: wrapperAt(path) });

      expect(getTrigger()).toHaveAttribute("aria-current", "page");
    },
  );

  it("reveals the dags, dag runs and task instances links on hover", async () => {
    render(<DagsButton authorizedMenuItems={["Dags"]} />, { wrapper: Wrapper });

    expect(screen.queryByRole("menuitem")).toBeNull();

    fireEvent.pointerEnter(getTrigger(), { pointerType: "mouse" });

    await waitFor(() => {
      expect(screen.getAllByRole("menuitem")).toHaveLength(3);
    });

    expect(screen.getAllByRole("menuitem").map((item) => item.getAttribute("href"))).toStrictEqual([
      "/dags",
      "/dag_runs",
      "/task_instances",
    ]);
  });

  it("ignores hover from a touch pointer", async () => {
    render(<DagsButton authorizedMenuItems={["Dags"]} />, { wrapper: Wrapper });

    fireEvent.pointerEnter(getTrigger(), { pointerType: "touch" });

    await expect(waitFor(() => expect(screen.getAllByRole("menuitem")).toHaveLength(3))).rejects.toThrow();
  });

  it("renders a plain button with no menu when Dags is not authorized", () => {
    render(<DagsButton authorizedMenuItems={[]} />, { wrapper: Wrapper });

    expect(getTrigger()).not.toHaveAttribute("aria-haspopup");
  });
});
