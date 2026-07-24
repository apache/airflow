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
import { render, screen } from "@testing-library/react";
import { ReactFlowProvider } from "@xyflow/react";
import type { ComponentProps, ReactNode } from "react";
import { MemoryRouter, Route, Routes } from "react-router-dom";
import { describe, expect, it, vi } from "vitest";

import { BaseWrapper, Wrapper } from "src/utils/Wrapper";

import { TaskNode } from "./TaskNode";
import { readableTextForFill } from "./nodeColors";
import type { CustomNodeProps } from "./reactflowUtils";

const RUN_MANUAL_SECTION_LABEL = "dags:runAndTaskActions.manualSection.button";

vi.mock("src/context/groups", () => ({
  useGroups: vi.fn(() => ({ toggleGroupId: vi.fn() })),
}));

const TestWrapper = ({ children }: { readonly children: ReactNode }) => (
  <Wrapper>
    <ReactFlowProvider>{children}</ReactFlowProvider>
  </Wrapper>
);

const TestWrapperWithDagRun = ({ children }: { readonly children: ReactNode }) => (
  <BaseWrapper>
    <MemoryRouter initialEntries={["/dags/example/runs/manual__2026-07-23T23:01:47/tasks/gate"]}>
      <Routes>
        <Route
          element={<ReactFlowProvider>{children}</ReactFlowProvider>}
          path="/dags/:dagId/runs/:runId/*"
        />
      </Routes>
    </MemoryRouter>
  </BaseWrapper>
);

// Chakra/Panda hashes color props into atomic class names rather than inline styles, so the resolved
// colour cannot be read back in jsdom. Instead we render two otherwise-identical nodes that differ
// only in the prop under test: any markup difference is attributable to that prop (hashing is
// deterministic), and identical markup proves the prop had no effect.
const renderHtml = (data: Partial<CustomNodeProps>): string => {
  const { container } = render(
    // The xyflow NodeProps surface is large; the component only reads `data` and `id`.
    <TaskNode
      {...({
        data: { height: 80, id: "t1", label: "t1", type: "task", width: 200, ...data },
      } as unknown as ComponentProps<typeof TaskNode>)}
    />,
    { wrapper: TestWrapper },
  );

  return container.innerHTML;
};

const renderNodeWithDagRun = (data: Partial<CustomNodeProps>) =>
  render(
    <TaskNode
      {...({
        data: { height: 80, id: "gate", label: "gate", type: "task", width: 200, ...data },
        id: "gate",
      } as unknown as ComponentProps<typeof TaskNode>)}
    />,
    { wrapper: TestWrapperWithDagRun },
  );

describe("TaskNode operator colors", () => {
  it("tints a leaf task when ui_color is set", () => {
    expect(renderHtml({ operator: "BashOperator", uiColor: "blue.500" })).not.toBe(
      renderHtml({ operator: "BashOperator" }),
    );
  });

  it("colors the operator text when ui_fgcolor is set", () => {
    expect(renderHtml({ operator: "BashOperator", uiFgcolor: "red.700" })).not.toBe(
      renderHtml({ operator: "BashOperator" }),
    );
  });

  it("tints a leaf task when ui_color is a raw hex color", () => {
    expect(renderHtml({ operator: "BashOperator", uiColor: "#e8b7e4" })).not.toBe(
      renderHtml({ operator: "BashOperator" }),
    );
  });

  it("tints a group node when ui_color is a token (2.x parity: ui_color is the group fill)", () => {
    expect(renderHtml({ isGroup: true, uiColor: "blue.500" })).not.toBe(renderHtml({ isGroup: true }));
  });

  it("alternates the group fill shade by nesting depth so nested groups stay distinct", () => {
    expect(renderHtml({ depth: 0, isGroup: true, isOpen: true, uiColor: "blue.500" })).not.toBe(
      renderHtml({ depth: 1, isGroup: true, isOpen: true, uiColor: "blue.500" }),
    );
  });
});

describe("TaskNode manual gate action", () => {
  const taskInstance = {
    child_states: {},
    dag_version_number: 1,
    has_note: false,
    max_end_date: null,
    min_start_date: "2026-07-23T23:01:47.000Z",
    state: "success",
    task_display_name: "gate",
    task_id: "gate",
  } as const;

  it("shows a trigger button for successful manual gate tasks", () => {
    renderNodeWithDagRun({
      operator: "ManualGateOperator",
      taskInstance,
    });

    expect(screen.getByRole("button", { name: RUN_MANUAL_SECTION_LABEL })).not.toBeNull();
  });

  it("does not show a trigger button for non-manual gate tasks", () => {
    renderNodeWithDagRun({
      operator: "BashOperator",
      taskInstance,
    });

    expect(screen.queryByRole("button", { name: RUN_MANUAL_SECTION_LABEL })).toBeNull();
  });

  it("does not show a trigger button for unfinished manual gate tasks", () => {
    renderNodeWithDagRun({
      operator: "ManualGateOperator",
      taskInstance: {
        ...taskInstance,
        state: "running",
      },
    });

    expect(screen.queryByRole("button", { name: RUN_MANUAL_SECTION_LABEL })).toBeNull();
  });

  it("does not show a trigger button for mapped manual gate tasks", () => {
    renderNodeWithDagRun({
      isMapped: true,
      operator: "ManualGateOperator",
      taskInstance,
    });

    expect(screen.queryByRole("button", { name: RUN_MANUAL_SECTION_LABEL })).toBeNull();
  });
});

describe("readableTextForFill", () => {
  it.each([
    { color: "#ffffff", expected: "black" },
    { color: "#fff", expected: "black" },
    { color: "#000000", expected: "gray.50" },
    { color: "#e8b7e4", expected: "black" },
    { color: "#1f77b4", expected: "gray.50" },
    { color: "blue.500", expected: undefined },
    { color: undefined, expected: undefined },
  ])("returns $expected for a fill of $color", ({ color, expected }) => {
    expect(readableTextForFill(color)).toBe(expected);
  });
});
