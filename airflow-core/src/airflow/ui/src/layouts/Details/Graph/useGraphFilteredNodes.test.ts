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
import { renderHook } from "@testing-library/react";
import type { Node as ReactFlowNode } from "@xyflow/react";
import { describe, expect, it } from "vitest";

import type { CustomNodeProps } from "src/components/Graph/reactflowUtils";

import { type GraphFilterValues, useGraphFilteredNodes } from "./useGraphFilteredNodes";

const makeNode = (
  overrides: { id?: string; type?: string } & Partial<CustomNodeProps> = {},
): ReactFlowNode<CustomNodeProps> => ({
  data: {
    id: overrides.id ?? "task_a",
    label: overrides.id ?? "task_a",
    type: overrides.type ?? "task",
    ...overrides,
  },
  id: overrides.id ?? "task_a",
  position: { x: 0, y: 0 },
  type: overrides.type ?? "task",
});

const noFilters: GraphFilterValues = {
  selectedOperators: [],
  selectedStates: [],
  selectedTaskGroups: [],
};

describe("useGraphFilteredNodes", () => {
  it("returns undefined when nodes are undefined", () => {
    const { result } = renderHook(() => useGraphFilteredNodes(undefined, noFilters));

    expect(result.current).toBeUndefined();
  });

  it("returns nodes unchanged when no filters are active", () => {
    const nodes = [makeNode({ id: "task_a" }), makeNode({ id: "task_b" })];
    const { result } = renderHook(() => useGraphFilteredNodes(nodes, noFilters));

    expect(result.current).toBe(nodes);
  });

  it("does not filter non-task nodes", () => {
    const joinNode: ReactFlowNode<CustomNodeProps> = {
      data: { id: "join_1", label: "join_1", type: "join" },
      id: "join_1",
      position: { x: 0, y: 0 },
      type: "join",
    };
    const filters: GraphFilterValues = { ...noFilters, selectedOperators: ["SomeOperator"] };
    const { result } = renderHook(() => useGraphFilteredNodes([joinNode], filters));

    expect(result.current?.[0]?.data.isFiltered).toBe(false);
  });

  it("operator filter: does not filter a node whose operator matches", () => {
    const node = makeNode({ operator: "BashOperator" });
    const filters: GraphFilterValues = { ...noFilters, selectedOperators: ["BashOperator"] };
    const { result } = renderHook(() => useGraphFilteredNodes([node], filters));

    expect(result.current?.[0]?.data.isFiltered).toBe(false);
  });

  it("operator filter: filters a node whose operator does not match", () => {
    const node = makeNode({ operator: "PythonOperator" });
    const filters: GraphFilterValues = { ...noFilters, selectedOperators: ["BashOperator"] };
    const { result } = renderHook(() => useGraphFilteredNodes([node], filters));

    expect(result.current?.[0]?.data.isFiltered).toBe(true);
  });

  it("operator filter: filters a node with no operator set", () => {
    const node = makeNode({ operator: undefined });
    const filters: GraphFilterValues = { ...noFilters, selectedOperators: ["BashOperator"] };
    const { result } = renderHook(() => useGraphFilteredNodes([node], filters));

    expect(result.current?.[0]?.data.isFiltered).toBe(true);
  });

  it("task group filter: does not filter a node that is the selected group itself", () => {
    const node = makeNode({ id: "group_a" });
    const filters: GraphFilterValues = { ...noFilters, selectedTaskGroups: ["group_a"] };
    const { result } = renderHook(() => useGraphFilteredNodes([node], filters));

    expect(result.current?.[0]?.data.isFiltered).toBe(false);
  });

  it("task group filter: does not filter a node whose id starts with the group prefix", () => {
    const node = makeNode({ id: "group_a.task_1" });
    const filters: GraphFilterValues = { ...noFilters, selectedTaskGroups: ["group_a"] };
    const { result } = renderHook(() => useGraphFilteredNodes([node], filters));

    expect(result.current?.[0]?.data.isFiltered).toBe(false);
  });

  it("task group filter: filters a node outside the selected group", () => {
    const node = makeNode({ id: "task_standalone" });
    const filters: GraphFilterValues = { ...noFilters, selectedTaskGroups: ["group_a"] };
    const { result } = renderHook(() => useGraphFilteredNodes([node], filters));

    expect(result.current?.[0]?.data.isFiltered).toBe(true);
  });

  it("state filter: does not filter a node whose task instance state matches", () => {
    const node = makeNode({
      taskInstance: {
        child_states: null,
        dag_version_number: undefined,
        max_end_date: null,
        min_start_date: null,
        state: "success",
        task_display_name: "task_a",
        task_id: "task_a",
      },
    });
    const filters: GraphFilterValues = { ...noFilters, selectedStates: ["success"] };
    const { result } = renderHook(() => useGraphFilteredNodes([node], filters));

    expect(result.current?.[0]?.data.isFiltered).toBe(false);
  });

  it("state filter: filters a node whose state does not match", () => {
    const node = makeNode({
      taskInstance: {
        child_states: null,
        dag_version_number: undefined,
        max_end_date: null,
        min_start_date: null,
        state: "failed",
        task_display_name: "task_a",
        task_id: "task_a",
      },
    });
    const filters: GraphFilterValues = { ...noFilters, selectedStates: ["success"] };
    const { result } = renderHook(() => useGraphFilteredNodes([node], filters));

    expect(result.current?.[0]?.data.isFiltered).toBe(true);
  });

  it('state filter: treats missing taskInstance state as "none"', () => {
    const node = makeNode({ taskInstance: undefined });
    const filters: GraphFilterValues = { ...noFilters, selectedStates: ["none"] };
    const { result } = renderHook(() => useGraphFilteredNodes([node], filters));

    expect(result.current?.[0]?.data.isFiltered).toBe(false);
  });

  it('state filter: filters a node with no taskInstance when state filter is not "none"', () => {
    const node = makeNode({ taskInstance: undefined });
    const filters: GraphFilterValues = { ...noFilters, selectedStates: ["success"] };
    const { result } = renderHook(() => useGraphFilteredNodes([node], filters));

    expect(result.current?.[0]?.data.isFiltered).toBe(true);
  });

  it("mapIndex filter: filters a non-mapped node when mapIndex is set", () => {
    const node = makeNode({ isMapped: false });
    const filters: GraphFilterValues = { ...noFilters, mapIndex: 0 };
    const { result } = renderHook(() => useGraphFilteredNodes([node], filters));

    expect(result.current?.[0]?.data.isFiltered).toBe(true);
  });

  it("mapIndex filter: filters a mapped node with no taskInstance when mapIndex is set", () => {
    const node = makeNode({ isMapped: true, taskInstance: undefined });
    const filters: GraphFilterValues = { ...noFilters, mapIndex: 0 };
    const { result } = renderHook(() => useGraphFilteredNodes([node], filters));

    expect(result.current?.[0]?.data.isFiltered).toBe(true);
  });

  it("mapIndex filter: does not filter a mapped node when mapIndex is within the total count", () => {
    const node = makeNode({
      isMapped: true,
      taskInstance: {
        child_states: { success: 3 },
        dag_version_number: undefined,
        max_end_date: null,
        min_start_date: null,
        state: "success",
        task_display_name: "task_a",
        task_id: "task_a",
      },
    });
    const filters: GraphFilterValues = { ...noFilters, mapIndex: 2 };
    const { result } = renderHook(() => useGraphFilteredNodes([node], filters));

    expect(result.current?.[0]?.data.isFiltered).toBe(false);
  });

  it("mapIndex filter: filters a mapped node when mapIndex is >= total count", () => {
    const node = makeNode({
      isMapped: true,
      taskInstance: {
        child_states: { success: 3 },
        dag_version_number: undefined,
        max_end_date: null,
        min_start_date: null,
        state: "success",
        task_display_name: "task_a",
        task_id: "task_a",
      },
    });
    const filters: GraphFilterValues = { ...noFilters, mapIndex: 3 };
    const { result } = renderHook(() => useGraphFilteredNodes([node], filters));

    expect(result.current?.[0]?.data.isFiltered).toBe(true);
  });

  it("durationThreshold filter: does not filter a node whose duration meets the threshold", () => {
    const node = makeNode({
      taskInstance: {
        child_states: null,
        dag_version_number: undefined,
        max_end_date: "2024-01-01T00:00:10Z",
        min_start_date: "2024-01-01T00:00:00Z",
        state: "success",
        task_display_name: "task_a",
        task_id: "task_a",
      },
    });
    const filters: GraphFilterValues = { ...noFilters, durationThreshold: 10 }; // exactly 10s
    const { result } = renderHook(() => useGraphFilteredNodes([node], filters));

    expect(result.current?.[0]?.data.isFiltered).toBe(false);
  });

  it("durationThreshold filter: filters a node whose duration is below the threshold", () => {
    const node = makeNode({
      taskInstance: {
        child_states: null,
        dag_version_number: undefined,
        max_end_date: "2024-01-01T00:00:05Z",
        min_start_date: "2024-01-01T00:00:00Z",
        state: "success",
        task_display_name: "task_a",
        task_id: "task_a",
      },
    });
    const filters: GraphFilterValues = { ...noFilters, durationThreshold: 10 }; // 5s < 10s
    const { result } = renderHook(() => useGraphFilteredNodes([node], filters));

    expect(result.current?.[0]?.data.isFiltered).toBe(true);
  });

  it("durationThreshold filter: filters a node with missing start/end dates", () => {
    const node = makeNode({
      taskInstance: {
        child_states: null,
        dag_version_number: undefined,
        max_end_date: null,
        min_start_date: null,
        state: "running",
        task_display_name: "task_a",
        task_id: "task_a",
      },
    });
    const filters: GraphFilterValues = { ...noFilters, durationThreshold: 5 };
    const { result } = renderHook(() => useGraphFilteredNodes([node], filters));

    expect(result.current?.[0]?.data.isFiltered).toBe(true);
  });

  it("multiple filters: filters a node that fails any active filter", () => {
    const node = makeNode({
      operator: "BashOperator",
      taskInstance: {
        child_states: null,
        dag_version_number: undefined,
        max_end_date: null,
        min_start_date: null,
        state: "failed",
        task_display_name: "task_a",
        task_id: "task_a",
      },
    });
    const filters: GraphFilterValues = {
      ...noFilters,
      selectedOperators: ["BashOperator"],
      selectedStates: ["success"],
    };
    const { result } = renderHook(() => useGraphFilteredNodes([node], filters));

    expect(result.current?.[0]?.data.isFiltered).toBe(true);
  });

  it("multiple filters: does not filter a node that passes all active filters", () => {
    const node = makeNode({
      operator: "BashOperator",
      taskInstance: {
        child_states: null,
        dag_version_number: undefined,
        max_end_date: "2024-01-01T00:00:15Z",
        min_start_date: "2024-01-01T00:00:00Z",
        state: "success",
        task_display_name: "task_a",
        task_id: "task_a",
      },
    });
    const filters: GraphFilterValues = {
      durationThreshold: 10,
      selectedOperators: ["BashOperator"],
      selectedStates: ["success"],
      selectedTaskGroups: [],
    };
    const { result } = renderHook(() => useGraphFilteredNodes([node], filters));

    expect(result.current?.[0]?.data.isFiltered).toBe(false);
  });
});
