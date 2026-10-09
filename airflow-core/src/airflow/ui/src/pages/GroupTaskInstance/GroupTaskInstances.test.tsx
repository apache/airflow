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
import { render } from "@testing-library/react";
import { MemoryRouter, Route, Routes } from "react-router-dom";
import { beforeEach, describe, expect, it, vi } from "vitest";

import { useIsLoopGroup } from "src/queries/useIsLoopGroup";
import { useLoopSummary } from "src/queries/useLoopSummary";

import { TaskInstances } from "../TaskInstances";
import { GroupTaskInstances } from "./GroupTaskInstances";

vi.mock("src/queries/useIsLoopGroup", () => ({ useIsLoopGroup: vi.fn() }));
vi.mock("src/queries/useLoopSummary", () => ({ useLoopSummary: vi.fn() }));
vi.mock("../TaskInstances", () => ({ TaskInstances: vi.fn(() => undefined) }));

const renderPage = () =>
  render(
    <MemoryRouter initialEntries={["/dags/dag/runs/run/tasks/group"]}>
      <Routes>
        <Route element={<GroupTaskInstances />} path="/dags/:dagId/runs/:runId/tasks/:groupId" />
      </Routes>
    </MemoryRouter>,
  );

describe("GroupTaskInstances", () => {
  beforeEach(() => {
    vi.mocked(TaskInstances).mockClear();
  });

  it.each([
    { label: "while the summary is loading", summary: { data: undefined } },
    { label: "when the summary failed to load", summary: { data: undefined, isError: true } },
  ])("filters a loop group's list by its id $label", ({ summary }) => {
    vi.mocked(useIsLoopGroup).mockReturnValue(true);
    vi.mocked(useLoopSummary).mockReturnValue(summary as ReturnType<typeof useLoopSummary>);

    renderPage();

    expect(TaskInstances).toHaveBeenCalledWith(
      expect.objectContaining({ extraFilter: undefined, loopGroupId: "group" }),
      undefined,
    );
  });

  it("does not filter a plain group's list by loop", () => {
    vi.mocked(useIsLoopGroup).mockReturnValue(false);
    vi.mocked(useLoopSummary).mockReturnValue({ data: undefined } as ReturnType<typeof useLoopSummary>);

    renderPage();

    expect(TaskInstances).toHaveBeenCalledWith(
      expect.objectContaining({ loopGroupId: undefined }),
      undefined,
    );
  });
});
