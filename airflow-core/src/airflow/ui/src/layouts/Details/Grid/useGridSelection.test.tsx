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
import type { MouseEvent, PropsWithChildren } from "react";

import { act, renderHook } from "@testing-library/react";
import { MemoryRouter, Route, Routes } from "react-router-dom";
import { describe, expect, it } from "vitest";

import type {
  GridNodeResponse,
  GridTISummaries,
  LightGridTaskInstanceSummary,
} from "openapi/requests/types.gen";

import { GridSelectionProvider, useHasGridSelection } from "src/context/gridSelection";
import { ShortcutRegistryProvider, useShortcutRegistry } from "src/context/keyboardShortcuts";

import { buildSelectableGroups, getCellKey, isSelectableCell, useGridSelection } from "./useGridSelection";
import type { GridTask } from "./utils";

const node = (id: string, isGroup = false): GridTask => ({
  depth: 0,
  id,
  is_mapped: false,
  isGroup,
  label: id,
});

const instance = (taskId: string, overrides: Partial<LightGridTaskInstanceSummary> = {}) => ({
  child_states: null,
  dag_version_number: 1,
  max_end_date: null,
  min_start_date: null,
  state: "success" as const,
  task_display_name: taskId,
  task_id: taskId,
  ...overrides,
});

const RUN_IDS = ["r1", "r2", "r3"];
const FLAT_NODES = [node("group", true), node("a"), node("b"), node("c")];
const SELECTABLE_GROUPS = new Map([["group", ["a", "b"]]]);
const LOOPED = { latest_region_id: "11111111-1111-4111-8111-111111111111", latest_region_index: 2 };

// "c" is looped in r2 and "b" has no task instance in r3, so neither can be selected.
const SUMMARIES = new Map<string, GridTISummaries>(
  RUN_IDS.map((runId) => [
    runId,
    {
      dag_id: "dag",
      run_id: runId,
      task_instances: [
        instance("group"),
        instance("a"),
        ...(runId === "r3" ? [] : [instance("b")]),
        instance("c", runId === "r2" ? LOOPED : {}),
      ],
    },
  ]),
);

const click = (modifiers: { ctrlKey?: boolean; metaKey?: boolean; shiftKey?: boolean } = {}) =>
  ({ ctrlKey: false, metaKey: false, shiftKey: false, ...modifiers }) as unknown as MouseEvent;

const renderSelection = (route = "/dags/dag") =>
  renderHook(
    () => ({
      ...useGridSelection({
        flatNodes: FLAT_NODES,
        runIds: RUN_IDS,
        selectableGroups: SELECTABLE_GROUPS,
        summariesByRunId: SUMMARIES,
      }),
      hasGridSelection: useHasGridSelection(),
      shortcuts: useShortcutRegistry().shortcuts,
    }),
    {
      wrapper: ({ children }: PropsWithChildren) => (
        <ShortcutRegistryProvider>
          <GridSelectionProvider>
            <MemoryRouter initialEntries={[route]}>
              <Routes>
                <Route element={children} path="/dags/:dagId" />
                <Route element={children} path="/dags/:dagId/runs/:runId/tasks/:taskId" />
                <Route element={children} path="/dags/:dagId/runs/:runId/tasks/group/:groupId" />
              </Routes>
            </MemoryRouter>
          </GridSelectionProvider>
        </ShortcutRegistryProvider>
      ),
    },
  );

const selectedKeys = (result: ReturnType<typeof renderSelection>["result"]) =>
  [...result.current.selection.keys()].sort();

const groupNode = (id: string, children: Array<GridNodeResponse>, isLoop = false): GridNodeResponse => ({
  children,
  id,
  is_loop: isLoop,
  is_mapped: false,
  label: id,
});
const leafNode = (id: string): GridNodeResponse => ({ id, is_mapped: false, label: id });

describe("buildSelectableGroups", () => {
  it("lists the tasks of each group, leaving out loops and the groups around or inside them", () => {
    const structure = [
      groupNode("outer", [leafNode("outer.a"), groupNode("outer.inner", [leafNode("outer.inner.b")])]),
      groupNode("with_loop", [leafNode("with_loop.c"), groupNode("with_loop.loop", [leafNode("d")], true)]),
      groupNode("loop", [groupNode("loop.group", [leafNode("loop.group.e")])], true),
      leafNode("f"),
    ];

    expect(buildSelectableGroups(structure)).toEqual(
      new Map([
        ["outer.inner", ["outer.inner.b"]],
        ["outer", ["outer.a", "outer.inner.b"]],
      ]),
    );
  });
});

describe("isSelectableCell", () => {
  it.each([
    { expected: true, name: "a plain task instance", taskInstance: instance("a"), taskNode: node("a") },
    { expected: true, name: "a group", taskInstance: instance("group"), taskNode: node("group", true) },
    {
      expected: false,
      name: "a group that is or holds a loop",
      taskInstance: instance("loop"),
      taskNode: node("loop", true),
    },
    { expected: false, name: "a cell without task instance", taskInstance: undefined, taskNode: node("a") },
    {
      expected: false,
      name: "a task that has no task instance in the run yet",
      taskInstance: instance("a", { dag_version_number: null }),
      taskNode: node("a"),
    },
    { expected: false, name: "a looped task", taskInstance: instance("c", LOOPED), taskNode: node("c") },
  ])("returns $expected for $name", ({ expected, taskInstance, taskNode }) => {
    expect(isSelectableCell(taskNode, taskInstance, SELECTABLE_GROUPS)).toBe(expected);
  });
});

describe("useGridSelection", () => {
  it("toggles a cell on Ctrl + click and Cmd + click without navigating", () => {
    const { result } = renderSelection();
    let handled = false;

    act(() => {
      handled = result.current.handleSelectionClick(click({ ctrlKey: true }), { runId: "r1", taskId: "a" });
    });

    expect(handled).toBe(true);
    expect(selectedKeys(result)).toEqual([getCellKey("r1", "a")]);
    expect(result.current.hasGridSelection).toBe(true);

    act(() => {
      result.current.handleSelectionClick(click({ metaKey: true }), { runId: "r1", taskId: "a" });
    });

    expect(selectedKeys(result)).toEqual([]);
    expect(result.current.hasGridSelection).toBe(false);
  });

  it("starts a selection from the opened task instance", () => {
    const { result } = renderSelection("/dags/dag/runs/r1/tasks/a");

    act(() => {
      result.current.handleSelectionClick(click({ ctrlKey: true }), { runId: "r2", taskId: "b" });
    });

    expect(selectedKeys(result)).toEqual([getCellKey("r1", "a"), getCellKey("r2", "b")].sort());
  });

  it("starts a selection from the opened task group", () => {
    const { result } = renderSelection("/dags/dag/runs/r1/tasks/group/group");

    act(() => {
      result.current.handleSelectionClick(click({ ctrlKey: true }), { runId: "r2", taskId: "group" });
    });

    expect(selectedKeys(result)).toEqual([getCellKey("r1", "group"), getCellKey("r2", "group")].sort());
  });

  it("selects only the opened task instance when it is the one Ctrl + clicked", () => {
    const { result } = renderSelection("/dags/dag/runs/r1/tasks/a");

    act(() => {
      result.current.handleSelectionClick(click({ ctrlKey: true }), { runId: "r1", taskId: "a" });
    });

    expect(selectedKeys(result)).toEqual([getCellKey("r1", "a")]);
  });

  it("selects the selectable cells of the rectangle on Shift + click", () => {
    const { result } = renderSelection();

    act(() => {
      result.current.handleSelectionClick(click({ ctrlKey: true }), { runId: "r1", taskId: "a" });
    });
    act(() => {
      result.current.handleSelectionClick(click({ shiftKey: true }), { runId: "r3", taskId: "c" });
    });

    expect(selectedKeys(result)).toEqual(
      [
        getCellKey("r1", "a"),
        getCellKey("r2", "a"),
        getCellKey("r3", "a"),
        getCellKey("r1", "b"),
        getCellKey("r2", "b"),
        getCellKey("r1", "c"),
        getCellKey("r3", "c"),
      ].sort(),
    );
  });

  it("anchors a Shift + click range on the opened task instance", () => {
    const { result } = renderSelection("/dags/dag/runs/r1/tasks/a");

    act(() => {
      result.current.handleSelectionClick(click({ shiftKey: true }), { runId: "r2", taskId: "b" });
    });

    expect(selectedKeys(result)).toEqual(
      [getCellKey("r1", "a"), getCellKey("r2", "a"), getCellKey("r1", "b"), getCellKey("r2", "b")].sort(),
    );
  });

  it("leaves a modified click on a cell that cannot be selected to the browser", () => {
    const { result } = renderSelection();
    let handled = true;

    act(() => {
      result.current.handleSelectionClick(click({ ctrlKey: true }), { runId: "r1", taskId: "a" });
    });
    act(() => {
      handled = result.current.handleSelectionClick(click({ ctrlKey: true }), { runId: "r2", taskId: "c" });
    });

    expect(handled).toBe(false);
    expect(selectedKeys(result)).toEqual([getCellKey("r1", "a")]);
  });

  it("clears the selection on a plain click and lets it navigate", () => {
    const { result } = renderSelection();
    let handled = true;

    act(() => {
      result.current.handleSelectionClick(click({ ctrlKey: true }), { runId: "r1", taskId: "a" });
    });
    act(() => {
      handled = result.current.handleSelectionClick(click(), { runId: "r2", taskId: "b" });
    });

    expect(handled).toBe(false);
    expect(selectedKeys(result)).toEqual([]);
  });

  it("lists the selection clicks in the keyboard shortcuts help while mounted", () => {
    const { result, unmount } = renderSelection();

    expect(result.current.shortcuts.map(({ category, keys }) => ({ category, keys }))).toEqual([
      { category: "dagView", keys: ["mod+Click"] },
      { category: "dagView", keys: ["shift+Click"] },
    ]);

    unmount();
  });

  it("deselects the given cells", () => {
    const { result } = renderSelection();

    act(() => {
      result.current.handleSelectionClick(click({ ctrlKey: true }), { runId: "r1", taskId: "a" });
    });
    act(() => {
      result.current.handleSelectionClick(click({ ctrlKey: true }), { runId: "r2", taskId: "b" });
    });
    act(() => {
      result.current.deselectCells([getCellKey("r1", "a")]);
    });

    expect(selectedKeys(result)).toEqual([getCellKey("r2", "b")]);
  });
});
