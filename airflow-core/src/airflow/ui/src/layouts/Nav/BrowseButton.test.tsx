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

import { BrowseButton } from "./BrowseButton";

const getTrigger = () => screen.getByRole("button", { name: "nav.browse" });

const openMenu = async () => {
  fireEvent.click(getTrigger());
  await waitFor(() => expect(screen.getAllByRole("menuitem").length).toBeGreaterThan(0));
};

const hrefs = () => screen.getAllByRole("menuitem").map((item) => item.getAttribute("href"));

const wrapperAt = (path: string) => {
  const wrapper = ({ children }: PropsWithChildren) => (
    <BaseWrapper>
      <MemoryRouter initialEntries={[path]}>{children}</MemoryRouter>
    </BaseWrapper>
  );

  return wrapper;
};

describe("BrowseButton", () => {
  it("lists Dag Runs and Task Instances for users who can see Dags", async () => {
    render(<BrowseButton authorizedMenuItems={["Dags"]} externalViews={[]} />, { wrapper: Wrapper });

    await openMenu();

    expect(hrefs()).toStrictEqual(["/dag_runs", "/task_instances"]);
  });

  it("hides Dag Runs and Task Instances without access to Dags", async () => {
    render(<BrowseButton authorizedMenuItems={["Jobs"]} externalViews={[]} />, { wrapper: Wrapper });

    await openMenu();

    expect(hrefs()).toStrictEqual(["/jobs"]);
  });

  it("keeps the entries in alphabetical order", async () => {
    render(<BrowseButton authorizedMenuItems={["Dags", "Jobs", "XComs"]} externalViews={[]} />, {
      wrapper: Wrapper,
    });

    await openMenu();

    expect(hrefs()).toStrictEqual(["/dag_runs", "/jobs", "/task_instances", "/xcoms"]);
  });

  it.each(["/dag_runs", "/task_instances"])("is highlighted on %s", (path) => {
    render(<BrowseButton authorizedMenuItems={["Dags"]} externalViews={[]} />, {
      wrapper: wrapperAt(path),
    });

    expect(getTrigger()).toHaveAttribute("aria-current", "page");
  });
});
