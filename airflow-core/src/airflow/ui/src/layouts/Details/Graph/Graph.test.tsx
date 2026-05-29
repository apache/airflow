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
import { act, render } from "@testing-library/react";
import { ReactFlow } from "@xyflow/react";
import { Children, isValidElement, type ComponentProps } from "react";
import { beforeEach, describe, expect, it, vi } from "vitest";

import { useDagRunServiceGetDagRun } from "openapi/queries";
import { useGraphLayout } from "src/components/Graph/useGraphLayout";
import { useGridTiSummariesStream } from "src/queries/useGridTISummaries.ts";
import { Wrapper } from "src/utils/Wrapper";

import { Graph } from "./Graph";
import { GraphControls } from "./components/GraphControls";

// testsSetup.ts globally mocks Graph to null so full-page tests don't need to
// stub ELK layout data. Unmock it here so we test the real component.
vi.unmock("src/layouts/Details/Graph/Graph");

let mockParams: Record<string, string> = { dagId: "test_dag" };

vi.mock("react-router-dom", async () => {
  const actual = await vi.importActual("react-router-dom");

  return {
    ...actual,
    useParams: () => mockParams,
    useSearchParams: () => [new URLSearchParams()],
  };
});

vi.mock("openapi/queries", () => ({
  useDagRunServiceGetDagRun: vi.fn(() => ({ data: undefined, isLoading: false })),
  useStructureServiceStructureData: vi.fn(() => ({ data: { edges: [], nodes: [] } })),
}));

vi.mock("src/queries/useDependencyGraph", () => ({
  useDependencyGraph: vi.fn(() => ({ data: { edges: [], nodes: [] } })),
}));

vi.mock("src/components/Graph/useGraphLayout", () => ({
  useGraphLayout: vi.fn(() => ({ data: { edges: [], nodes: [] } })),
}));

vi.mock("src/queries/useGridTISummaries.ts", () => ({
  useGridTiSummariesStream: vi.fn(() => ({ summariesByRunId: new Map() })),
}));

vi.mock("src/hooks/useSelectedVersion", () => ({
  default: vi.fn(() => 1),
}));

vi.mock("src/context/groups", () => ({
  useGroups: vi.fn(() => ({ allGroupIds: [], openGroupIds: [], setAllGroupIds: vi.fn() })),
}));

vi.mock("@xyflow/react", async () => {
  const actual = await vi.importActual("@xyflow/react");

  return {
    ...actual,
    ReactFlow: vi.fn(() => null),
    useReactFlow: vi.fn(() => ({ fitView: vi.fn(), getZoom: vi.fn() })),
  };
});

const mockDagRun = {
  bundle_version: null,
  conf: null,
  dag_display_name: "test_dag",
  dag_id: "test_dag",
  dag_versions: [],
  end_date: null,
  has_missed_deadline: false,
  logical_date: null,
  note: null,
  partition_key: null,
  queued_at: null,
  run_after: "2025-01-01T00:00:00Z",
  run_id: "run_1",
  run_type: "manual" as const,
  start_date: "2025-01-01T00:00:01Z",
  state: "running" as const,
  triggered_by: "ui" as const,
  triggering_user_name: null,
};
const mockGraphNode = {
  data: { id: "task_1", label: "Task 1", type: "task" },
  height: 80,
  id: "task_1",
  position: { x: 10, y: 20 },
  type: "task",
  width: 100,
};
const mockGraphNodeTwo = {
  data: { id: "task_2", label: "Task 2", type: "task" },
  height: 80,
  id: "task_2",
  position: { x: 130, y: 20 },
  type: "task",
  width: 100,
};
const mockGraphEdge = {
  data: {
    rest: {
      id: "edge-task-1-task-2",
      sources: ["task_1"],
      targets: ["task_2"],
    },
  },
  id: "edge-task-1-task-2",
  source: "task_1",
  target: "task_2",
  type: "custom",
};
const changedNodePosition = { x: 520, y: 260 };
const freeNodePosition = { x: 420, y: 420 };
const overlappingNodePosition = { x: 130, y: 20 };
const simultaneousDropPosition = { x: 130, y: 20 };
const testSearchGridSize = 24;
const testMaxSearchRadius = 24;
const testRadialDirections = [
  { x: 1, y: 0 },
  { x: -1, y: 0 },
  { x: 0, y: 1 },
  { x: 0, y: -1 },
  { x: 1, y: 1 },
  { x: 1, y: -1 },
  { x: -1, y: 1 },
  { x: -1, y: -1 },
];

const nodesOverlap = ({
  first,
  second,
}: {
  first: { height?: number; position: { x: number; y: number }; width?: number };
  second: { height?: number; position: { x: number; y: number }; width?: number };
}) =>
  first.position.x < second.position.x + (second.width ?? 0) &&
  first.position.x + (first.width ?? 0) > second.position.x &&
  first.position.y < second.position.y + (second.height ?? 0) &&
  first.position.y + (first.height ?? 0) > second.position.y;

const getGraphControlsProps = () => {
  const children = vi.mocked(ReactFlow).mock.lastCall?.[0]?.children;
  let graphControlsProps: ComponentProps<typeof GraphControls> | undefined;

  Children.forEach(children, (child) => {
    if (isValidElement(child) && child.type === GraphControls) {
      graphControlsProps = child.props as ComponentProps<typeof GraphControls>;
    }
  });

  return graphControlsProps;
};

const getRenderedNodes = () => vi.mocked(ReactFlow).mock.lastCall?.[0]?.nodes ?? [];

const getRenderedNodeById = (nodeId: string) => getRenderedNodes().find((node) => node.id === nodeId);

describe("Graph", () => {
  beforeEach(() => {
    mockParams = { dagId: "test_dag" };
    localStorage.clear();
    vi.mocked(ReactFlow).mockClear();
    vi.mocked(useGraphLayout).mockReturnValue({
      data: { edges: [mockGraphEdge], nodes: [mockGraphNode, mockGraphNodeTwo] },
      isPending: false,
    } as ReturnType<typeof useGraphLayout>);
  });

  it("passes states to useGridTiSummariesStream when a runId is present", () => {
    mockParams = { dagId: "test_dag", runId: "run_1" };
    vi.mocked(useDagRunServiceGetDagRun).mockReturnValue({
      data: mockDagRun,
      isLoading: false,
    } as ReturnType<typeof useDagRunServiceGetDagRun>);

    render(<Graph />, { wrapper: Wrapper });

    expect(vi.mocked(useGridTiSummariesStream)).toHaveBeenCalledWith(
      expect.objectContaining({
        runIds: ["run_1"],
        states: ["running"],
      }),
    );
  });

  it("passes undefined states while dag run is still loading", () => {
    mockParams = { dagId: "test_dag", runId: "run_1" };
    vi.mocked(useDagRunServiceGetDagRun).mockReturnValue({
      data: undefined,
      isLoading: true,
    } as ReturnType<typeof useDagRunServiceGetDagRun>);

    render(<Graph />, { wrapper: Wrapper });

    expect(vi.mocked(useGridTiSummariesStream)).toHaveBeenCalledWith(
      expect.objectContaining({
        runIds: ["run_1"],
        states: undefined,
      }),
    );
  });

  it("passes undefined states to useGridTiSummariesStream when there is no runId", () => {
    mockParams = { dagId: "test_dag" };
    vi.mocked(useDagRunServiceGetDagRun).mockReturnValue({
      data: undefined,
      isLoading: false,
    } as ReturnType<typeof useDagRunServiceGetDagRun>);

    render(<Graph />, { wrapper: Wrapper });

    expect(vi.mocked(useGridTiSummariesStream)).toHaveBeenCalledWith(
      expect.objectContaining({
        runIds: [],
        states: undefined,
      }),
    );
  });

  it("sets manual layout edge mode only while manual mode is active", () => {
    render(<Graph />, { wrapper: Wrapper });

    expect(vi.mocked(ReactFlow).mock.lastCall?.[0]?.edges?.[0]?.data?.isManualLayout).toBe(false);

    act(() => {
      getGraphControlsProps()?.onToggleManualLayout();
    });

    expect(vi.mocked(ReactFlow).mock.lastCall?.[0]?.edges?.[0]?.data?.isManualLayout).toBe(true);

    act(() => {
      getGraphControlsProps()?.onToggleManualLayout();
    });

    expect(vi.mocked(ReactFlow).mock.lastCall?.[0]?.edges?.[0]?.data?.isManualLayout).toBe(false);
  });

  it("restores ELK positions when switching manual layout off", () => {
    render(<Graph />, { wrapper: Wrapper });

    act(() => {
      getGraphControlsProps()?.onToggleManualLayout();
    });

    const onNodesChange = vi.mocked(ReactFlow).mock.lastCall?.[0]?.onNodesChange;

    act(() => {
      onNodesChange?.([{ dragging: false, id: "task_1", position: changedNodePosition, type: "position" }]);
    });

    expect(vi.mocked(ReactFlow).mock.lastCall?.[0]?.nodes).toEqual(
      expect.arrayContaining([expect.objectContaining({ id: "task_1", position: changedNodePosition })]),
    );

    act(() => {
      getGraphControlsProps()?.onToggleManualLayout();
    });

    expect(vi.mocked(ReactFlow).mock.lastCall?.[0]?.nodesDraggable).toBe(false);
    expect(vi.mocked(ReactFlow).mock.lastCall?.[0]?.nodes).toEqual(
      expect.arrayContaining([expect.objectContaining({ id: "task_1", position: { x: 10, y: 20 } })]),
    );
  });

  it("keeps the dropped position when releasing with no overlap", () => {
    render(<Graph />, { wrapper: Wrapper });

    act(() => {
      getGraphControlsProps()?.onToggleManualLayout();
    });

    const onNodesChange = vi.mocked(ReactFlow).mock.lastCall?.[0]?.onNodesChange;

    act(() => {
      onNodesChange?.([{ dragging: true, id: "task_1", position: freeNodePosition, type: "position" }]);
    });

    expect(getRenderedNodeById("task_1")?.position).toEqual(freeNodePosition);

    act(() => {
      onNodesChange?.([{ dragging: false, id: "task_1", position: freeNodePosition, type: "position" }]);
    });

    expect(getRenderedNodeById("task_1")?.position).toEqual(freeNodePosition);
  });

  it("resolves simultaneous drop changes deterministically", () => {
    render(<Graph />, { wrapper: Wrapper });

    act(() => {
      getGraphControlsProps()?.onToggleManualLayout();
    });

    const onNodesChange = vi.mocked(ReactFlow).mock.lastCall?.[0]?.onNodesChange;

    act(() => {
      onNodesChange?.([
        { dragging: false, id: "task_1", position: simultaneousDropPosition, type: "position" },
        { dragging: false, id: "task_2", position: simultaneousDropPosition, type: "position" },
      ]);
    });

    let firstNode = getRenderedNodeById("task_1");
    let secondNode = getRenderedNodeById("task_2");
    const firstPassHasOverlap =
      firstNode !== undefined && secondNode !== undefined
        ? nodesOverlap({ first: firstNode, second: secondNode })
        : true;

    expect(firstPassHasOverlap).toBe(false);

    const firstPassPositions = {
      task1: firstNode?.position,
      task2: secondNode?.position,
    };

    act(() => {
      getGraphControlsProps()?.onToggleManualLayout();
      getGraphControlsProps()?.onToggleManualLayout();
    });

    const rerunOnNodesChange = vi.mocked(ReactFlow).mock.lastCall?.[0]?.onNodesChange;

    act(() => {
      rerunOnNodesChange?.([
        { dragging: false, id: "task_1", position: simultaneousDropPosition, type: "position" },
        { dragging: false, id: "task_2", position: simultaneousDropPosition, type: "position" },
      ]);
    });

    firstNode = getRenderedNodeById("task_1");
    secondNode = getRenderedNodeById("task_2");
    const secondPassPositions = {
      task1: firstNode?.position,
      task2: secondNode?.position,
    };

    expect(secondPassPositions).toEqual(firstPassPositions);
  });

  it("keeps the dropped position when no free slot is found within the search radius", () => {
    const blockedDropPosition = { x: 100, y: 100 };
    const blockedPositions = [blockedDropPosition];

    for (let radius = 1; radius <= testMaxSearchRadius; radius += 1) {
      for (const direction of testRadialDirections) {
        blockedPositions.push({
          x: blockedDropPosition.x + direction.x * radius * testSearchGridSize,
          y: blockedDropPosition.y + direction.y * radius * testSearchGridSize,
        });
      }
    }

    const blockingNodes = blockedPositions.map((position, index) => ({
      data: { id: `task_blocker_${index}`, label: `Task Blocker ${index}`, type: "task" },
      height: 80,
      id: `task_blocker_${index}`,
      position,
      type: "task",
      width: 100,
    }));

    vi.mocked(useGraphLayout).mockReturnValue({
      data: { edges: [mockGraphEdge], nodes: [mockGraphNode, ...blockingNodes] },
      isPending: false,
    } as ReturnType<typeof useGraphLayout>);

    render(<Graph />, { wrapper: Wrapper });

    act(() => {
      getGraphControlsProps()?.onToggleManualLayout();
    });

    const onNodesChange = vi.mocked(ReactFlow).mock.lastCall?.[0]?.onNodesChange;

    act(() => {
      onNodesChange?.([{ dragging: false, id: "task_1", position: blockedDropPosition, type: "position" }]);
    });

    expect(getRenderedNodeById("task_1")?.position).toEqual(blockedDropPosition);
  });

  it("keeps overlap during drag preview and snaps to a nearby open position on release", () => {
    render(<Graph />, { wrapper: Wrapper });

    act(() => {
      getGraphControlsProps()?.onToggleManualLayout();
    });

    const onNodesChange = vi.mocked(ReactFlow).mock.lastCall?.[0]?.onNodesChange;

    act(() => {
      onNodesChange?.([
        { dragging: true, id: "task_1", position: overlappingNodePosition, type: "position" },
      ]);
    });

    let renderedNodes = vi.mocked(ReactFlow).mock.lastCall?.[0]?.nodes ?? [];
    let firstNode = renderedNodes.find((node) => node.id === "task_1");
    let secondNode = renderedNodes.find((node) => node.id === "task_2");
    let hasOverlap =
      firstNode !== undefined && secondNode !== undefined
        ? nodesOverlap({ first: firstNode, second: secondNode })
        : true;

    expect(firstNode?.position).toEqual(overlappingNodePosition);
    expect(hasOverlap).toBe(true);

    act(() => {
      onNodesChange?.([
        { dragging: false, id: "task_1", position: overlappingNodePosition, type: "position" },
      ]);
    });

    renderedNodes = vi.mocked(ReactFlow).mock.lastCall?.[0]?.nodes ?? [];
    firstNode = renderedNodes.find((node) => node.id === "task_1");
    secondNode = renderedNodes.find((node) => node.id === "task_2");
    hasOverlap =
      firstNode !== undefined && secondNode !== undefined
        ? nodesOverlap({ first: firstNode, second: secondNode })
        : true;

    expect(firstNode?.position).not.toEqual(overlappingNodePosition);
    expect(hasOverlap).toBe(false);
  });

  it("forces manual layout mode back to auto after component re-mount", () => {
    const { unmount } = render(<Graph />, { wrapper: Wrapper });

    act(() => {
      getGraphControlsProps()?.onToggleManualLayout();
    });

    expect(vi.mocked(ReactFlow).mock.lastCall?.[0]?.nodesDraggable).toBe(true);
    unmount();

    render(<Graph />, { wrapper: Wrapper });

    expect(vi.mocked(ReactFlow).mock.lastCall?.[0]?.nodesDraggable).toBe(false);
    expect(getGraphControlsProps()?.isManualLayout).toBe(false);
  });
});
