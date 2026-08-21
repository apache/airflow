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
import { cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import type { PropsWithChildren } from "react";
import { MemoryRouter, Route, Routes, useLocation } from "react-router-dom";
import { afterEach, describe, expect, it } from "vitest";

import { BaseWrapper } from "src/utils/Wrapper";

import { DagBreadcrumb } from "./DagBreadcrumb";

// The msw dag-details handler is pinned to this dag id.
const DAG_ID = "tutorial_taskflow_api";

const LocationProbe = () => <div data-testid="location-pathname">{useLocation().pathname}</div>;

const createWrapper =
  (path: string, route: string) =>
  ({ children }: PropsWithChildren) => (
    <BaseWrapper>
      <MemoryRouter initialEntries={[path]}>
        <Routes>
          <Route
            element={
              <>
                {children}
                <LocationProbe />
              </>
            }
            path={route}
          />
        </Routes>
      </MemoryRouter>
    </BaseWrapper>
  );

afterEach(() => cleanup());

describe("DagBreadcrumb", () => {
  it("never renders the pause toggle at the Dag level", async () => {
    render(<DagBreadcrumb />, { wrapper: createWrapper(`/dags/${DAG_ID}`, "/dags/:dagId") });

    await screen.findByRole("link", { name: new RegExp(DAG_ID, "u") });

    expect(screen.queryByTestId("toggle-pause")).not.toBeInTheDocument();
  });

  it("links the Dag level to the Dag page and offers the switcher", async () => {
    render(<DagBreadcrumb />, { wrapper: createWrapper(`/dags/${DAG_ID}`, "/dags/:dagId") });

    expect(await screen.findByRole("link", { name: new RegExp(DAG_ID, "u") })).toHaveAttribute(
      "href",
      `/dags/${DAG_ID}`,
    );
    expect(screen.getByTestId("switch-dag")).toBeInTheDocument();
  });

  it("switches Dag from the chevron dropdown", async () => {
    render(<DagBreadcrumb />, { wrapper: createWrapper(`/dags/${DAG_ID}`, "/dags/:dagId") });

    fireEvent.click(await screen.findByTestId("switch-dag"));

    const option = await screen.findByText(`${DAG_ID}_success`, undefined, { timeout: 3000 });

    fireEvent.click(option);

    await waitFor(() =>
      expect(screen.getByTestId("location-pathname")).toHaveTextContent(`/dags/${DAG_ID}_success`),
    );
  });

  it("renders one level per URL segment, with only the current one unlinked", async () => {
    render(<DagBreadcrumb />, {
      wrapper: createWrapper(
        `/dags/${DAG_ID}/runs/run_1/tasks/task_1/mapped/0`,
        "/dags/:dagId/runs/:runId/tasks/:taskId/mapped/:mapIndex",
      ),
    });

    await screen.findByRole("link", { name: new RegExp(DAG_ID, "u") });

    for (const caption of ["dag_one", "dagRun_one", "taskInstance_other", "mapIndex"]) {
      expect(screen.getByText(caption)).toBeInTheDocument();
    }

    // The caption sits inside the link, so it is part of the accessible name too.
    expect(screen.getByRole("link", { name: /run_1/u })).toHaveAttribute(
      "href",
      `/dags/${DAG_ID}/runs/run_1`,
    );
    // A map index in the URL means the instances are expanded, so the task level links to the list
    // that sits between the run and this instance — even for a task whose own `is_mapped` is false,
    // as happens inside a mapped task group.
    expect(screen.getByRole("link", { name: /task_1/u })).toHaveAttribute(
      "href",
      `/dags/${DAG_ID}/runs/run_1/tasks/task_1/mapped`,
    );
    // The map index is the page's own level, so it is text rather than a link.
    expect(screen.queryByRole("link", { name: "0" })).not.toBeInTheDocument();
    expect(screen.getByText("0")).toBeInTheDocument();
  });
});
