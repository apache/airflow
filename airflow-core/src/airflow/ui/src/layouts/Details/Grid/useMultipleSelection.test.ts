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
import { act, renderHook } from "@testing-library/react";
import type { GridTask } from "./utils";
import type { LightGridTaskInstanceSummary } from "openapi/requests/types.gen";

import useMultipleSelection from "./useMultipleSelection";

const createMockFlatNodes = (): Array<GridTask> => [
  { id: "task_a", depth: 0, label: "Task A", isGroup: false, is_mapped: false },
  { id: "task_b", depth: 0, label: "Task B", isGroup: false, is_mapped: false },
  { id: "task_c", depth: 0, label: "Task C", isGroup: false, is_mapped: false },
  { id: "task_d", depth: 0, label: "Task D", isGroup: false, is_mapped: false },
];

const createMockRunIds = (): Array<string> => ["run1", "run2", "run3"];

const createMockGetTaskInstance = () => {
  const instances = new Map<string, LightGridTaskInstanceSummary>();

  createMockFlatNodes().forEach((node) => {
    createMockRunIds().forEach((runId) => {
      instances.set(`${node.id}-${runId}`, {
        task_id: node.id,
        task_display_name: node.label,
        state: "success",
        child_states: null,
        min_start_date: null,
        max_end_date: null,
        dag_version_number: 1,
        has_note: false,
        loop_iterations_count: null,
        latest_region_id: null,
        latest_region_index: null,
      });
    });
  });

  return (taskId: string, runId: string) => instances.get(`${taskId}-${runId}`);
};

describe("useMultipleSelection", () => {
  const flatNodes = createMockFlatNodes();
  const runIds = createMockRunIds();
  const getTaskInstance = createMockGetTaskInstance();

  test("Initial values", () => {
    const { result } = renderHook(() => useMultipleSelection(flatNodes, runIds, getTaskInstance));

    expect(result.current.selectedTaskInstances).toEqual([]);
    expect(result.current.selectionCount).toBe(0);
    expect(result.current.hasSelection).toBe(false);
  });

  test("Simple click selects single task instance", () => {
    const { result } = renderHook(() => useMultipleSelection(flatNodes, runIds, getTaskInstance));

    act(() => {
      result.current.handleClick("task_a", "run1", { ctrlKey: false, shiftKey: false, metaKey: false } as any);
    });

    expect(result.current.selectionCount).toBe(1);
    expect(result.current.isSelected("task_a", "run1")).toBe(true);
  });

  test("Ctrl+Click adds multiple task instances", () => {
    const { result } = renderHook(() => useMultipleSelection(flatNodes, runIds, getTaskInstance));

    act(() => {
      result.current.handleClick("task_a", "run1", { ctrlKey: true, shiftKey: false, metaKey: false } as any);
      result.current.handleClick("task_b", "run2", { ctrlKey: true, shiftKey: false, metaKey: false } as any);
    });

    expect(result.current.selectionCount).toBe(2);
    expect(result.current.isSelected("task_a", "run1")).toBe(true);
    expect(result.current.isSelected("task_b", "run2")).toBe(true);
  });

  test("Ctrl+Click on already selected task instance removes it", () => {
    const { result } = renderHook(() => useMultipleSelection(flatNodes, runIds, getTaskInstance));

    act(() => {
      result.current.handleClick("task_a", "run1", { ctrlKey: true, shiftKey: false, metaKey: false } as any);
      result.current.handleClick("task_a", "run1", { ctrlKey: true, shiftKey: false, metaKey: false } as any);
    });

    expect(result.current.selectionCount).toBe(0);
    expect(result.current.isSelected("task_a", "run1")).toBe(false);
  });

  test("Shift+Click selects range between anchor and target", () => {
    const { result } = renderHook(() => useMultipleSelection(flatNodes, runIds, getTaskInstance));

    // First click sets anchor
    act(() => {
      result.current.handleClick("task_a", "run1", { ctrlKey: false, shiftKey: false, metaKey: false } as any);
    });

    // Shift+Click on task_c, run3 selects range
    act(() => {
      result.current.handleClick("task_c", "run3", { ctrlKey: false, shiftKey: true, metaKey: false } as any);
    });

    // Should select all tasks from task_a to task_c and run1 to run3
    // That's 3 tasks x 3 runs = 9 task instances
    expect(result.current.selectionCount).toBe(9);
    expect(result.current.isSelected("task_a", "run1")).toBe(true);
    expect(result.current.isSelected("task_b", "run2")).toBe(true);
    expect(result.current.isSelected("task_c", "run3")).toBe(true);
  });

  test("Clear selection removes all selected task instances", () => {
    const { result } = renderHook(() => useMultipleSelection(flatNodes, runIds, getTaskInstance));

    act(() => {
      result.current.handleClick("task_a", "run1", { ctrlKey: true, shiftKey: false, metaKey: false } as any);
      result.current.handleClick("task_b", "run2", { ctrlKey: true, shiftKey: false, metaKey: false } as any);
    });

    expect(result.current.selectionCount).toBe(2);

    act(() => {
      result.current.clearSelection();
    });

    expect(result.current.selectionCount).toBe(0);
    expect(result.current.hasSelection).toBe(false);
  });

  test("isColumnSelected returns true when any task in column is selected", () => {
    const { result } = renderHook(() => useMultipleSelection(flatNodes, runIds, getTaskInstance));

    act(() => {
      result.current.handleClick("task_a", "run1", { ctrlKey: true, shiftKey: false, metaKey: false } as any);
    });

    expect(result.current.isColumnSelected("run1")).toBe(true);
    expect(result.current.isColumnSelected("run2")).toBe(false);
  });

  test("getSelectedForRun returns selected task instances for a specific run", () => {
    const { result } = renderHook(() => useMultipleSelection(flatNodes, runIds, getTaskInstance));

    act(() => {
      result.current.handleClick("task_a", "run1", { ctrlKey: true, shiftKey: false, metaKey: false } as any);
      result.current.handleClick("task_b", "run1", { ctrlKey: true, shiftKey: false, metaKey: false } as any);
      result.current.handleClick("task_c", "run2", { ctrlKey: true, shiftKey: false, metaKey: false } as any);
    });

    const run1Selection = result.current.getSelectedForRun("run1");
    expect(run1Selection).toHaveLength(2);
    expect(run1Selection.map((s) => s.taskId).sort()).toEqual(["task_a", "task_b"]);
  });

  test("Meta key (Cmd on Mac) works like Ctrl key", () => {
    const { result } = renderHook(() => useMultipleSelection(flatNodes, runIds, getTaskInstance));

    act(() => {
      result.current.handleClick("task_a", "run1", { ctrlKey: false, shiftKey: false, metaKey: true } as any);
      result.current.handleClick("task_b", "run2", { ctrlKey: false, shiftKey: false, metaKey: true } as any);
    });

    expect(result.current.selectionCount).toBe(2);
    expect(result.current.isSelected("task_a", "run1")).toBe(true);
    expect(result.current.isSelected("task_b", "run2")).toBe(true);
  });

  test("Click without modifiers clears previous selection", () => {
    const { result } = renderHook(() => useMultipleSelection(flatNodes, runIds, getTaskInstance));

    act(() => {
      result.current.handleClick("task_a", "run1", { ctrlKey: true, shiftKey: false, metaKey: false } as any);
      result.current.handleClick("task_b", "run2", { ctrlKey: true, shiftKey: false, metaKey: false } as any);
    });

    expect(result.current.selectionCount).toBe(2);

    // Click without modifiers should clear and select only the new one
    act(() => {
      result.current.handleClick("task_c", "run3", { ctrlKey: false, shiftKey: false, metaKey: false } as any);
    });

    expect(result.current.selectionCount).toBe(1);
    expect(result.current.isSelected("task_c", "run3")).toBe(true);
    expect(result.current.isSelected("task_a", "run1")).toBe(false);
  });
});