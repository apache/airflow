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
import { clearAllManualLayoutStates, scheduleClearManualLayoutStatesForDag } from "./utils/manualLayoutStore";

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
const mockGraphNodeThree = {
  data: { id: "task_3", label: "Task 3", type: "task" },
  height: 80,
  id: "task_3",
  position: { x: 10, y: 180 },
  type: "task",
  width: 100,
};
const mockGraphNodeFour = {
  data: { id: "task_4", label: "Task 4", type: "task" },
  height: 80,
  id: "task_4",
  position: { x: 130, y: 180 },
  type: "task",
  width: 100,
};
const mockGraphGroupNode = {
  data: { id: "section_1", isGroup: true, isOpen: true, label: "Section 1", type: "task" },
  height: 200,
  id: "section_1",
  position: { x: 100, y: 100 },
  type: "task",
  width: 300,
};
const mockGraphGroupChildNode = {
  data: { id: "section_1.task_1", label: "Task 1", type: "task" },
  height: 80,
  id: "section_1.task_1",
  parentId: "section_1",
  position: { x: 130, y: 160 },
  type: "task",
  width: 100,
};
const mockAssetConditionNode = {
  data: {
    assetCondition: "and-gate",
    id: "asset_condition_1",
    label: "Asset Expression",
    type: "asset-condition",
  },
  height: 30,
  id: "asset_condition_1",
  position: { x: 250, y: 120 },
  type: "asset-condition",
  width: 30,
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
const mockUnrelatedGraphEdge = {
  data: {
    rest: {
      id: "edge-task-3-task-4",
      sources: ["task_3"],
      targets: ["task_4"],
    },
  },
  id: "edge-task-3-task-4",
  source: "task_3",
  target: "task_4",
  type: "custom",
};
const mockGraphGroupChildEdge = {
  data: {
    rest: {
      id: "edge-section-1-task-1-task-2",
      sources: ["section_1.task_1"],
      targets: ["task_2"],
    },
  },
  id: "edge-section-1-task-1-task-2",
  source: "section_1.task_1",
  target: "task_2",
  type: "custom",
};
const mockAssetConditionEdge = {
  data: {
    rest: {
      id: "edge-asset-condition-1-task-1",
      sources: ["asset_condition_1"],
      targets: ["task_1"],
    },
  },
  id: "edge-asset-condition-1-task-1",
  source: "asset_condition_1",
  target: "task_1",
  type: "custom",
};
const changedNodePosition = { x: 520, y: 260 };
const changedGroupPosition = { x: 160, y: 140 };
const changedAssetConditionPosition = { x: 300, y: 180 };
const overlappingNodePosition = { x: 130, y: 20 };
const changedViewport = { x: -240, y: -160, zoom: 0.75 };

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

const getRenderedEdges = () => vi.mocked(ReactFlow).mock.lastCall?.[0]?.edges ?? [];

const getRenderedEdgeById = (edgeId: string) => getRenderedEdges().find((edge) => edge.id === edgeId);

describe("Graph", () => {
  beforeEach(() => {
    mockParams = { dagId: "test_dag" };
    localStorage.clear();
    clearAllManualLayoutStates();
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

  it("keeps ELK edge layout until a node moves in manual mode", () => {
    vi.mocked(useGraphLayout).mockReturnValue({
      data: {
        edges: [mockGraphEdge, mockUnrelatedGraphEdge],
        nodes: [mockGraphNode, mockGraphNodeTwo, mockGraphNodeThree, mockGraphNodeFour],
      },
      isPending: false,
    } as ReturnType<typeof useGraphLayout>);

    render(<Graph />, { wrapper: Wrapper });

    expect(getRenderedEdgeById("edge-task-1-task-2")?.data?.isManualLayout).toBe(false);
    expect(getRenderedEdgeById("edge-task-3-task-4")?.data?.isManualLayout).toBe(false);
    expect(vi.mocked(ReactFlow).mock.lastCall?.[0]?.defaultEdgeOptions).toEqual({ zIndex: 1 });

    act(() => {
      getGraphControlsProps()?.onToggleManualLayout();
    });

    expect(getRenderedEdgeById("edge-task-1-task-2")?.data?.isManualLayout).toBe(false);
    expect(getRenderedEdgeById("edge-task-3-task-4")?.data?.isManualLayout).toBe(false);
    expect(vi.mocked(ReactFlow).mock.lastCall?.[0]?.defaultEdgeOptions).toEqual({
      interactionWidth: 0,
      zIndex: 1,
    });

    const onNodesChange = vi.mocked(ReactFlow).mock.lastCall?.[0]?.onNodesChange;

    act(() => {
      onNodesChange?.([{ dragging: true, id: "task_1", position: changedNodePosition, type: "position" }]);
    });

    expect(getRenderedEdgeById("edge-task-1-task-2")?.data?.isManualLayout).toBe(true);
    expect(getRenderedEdgeById("edge-task-3-task-4")?.data?.isManualLayout).toBe(false);

    act(() => {
      getGraphControlsProps()?.onToggleManualLayout();
    });

    expect(getRenderedEdgeById("edge-task-1-task-2")?.data?.isManualLayout).toBe(false);
    expect(getRenderedEdgeById("edge-task-3-task-4")?.data?.isManualLayout).toBe(false);

    act(() => {
      getGraphControlsProps()?.onToggleManualLayout();
    });

    expect(getRenderedEdgeById("edge-task-1-task-2")?.data?.isManualLayout).toBe(false);
    expect(getRenderedEdgeById("edge-task-3-task-4")?.data?.isManualLayout).toBe(false);
  });

  it("keeps open task group children in parent coordinates while moving the group", () => {
    vi.mocked(useGraphLayout).mockReturnValue({
      data: {
        edges: [mockGraphGroupChildEdge],
        nodes: [mockGraphGroupNode, mockGraphGroupChildNode, mockGraphNodeTwo],
      },
      isPending: false,
    } as unknown as ReturnType<typeof useGraphLayout>);

    render(<Graph />, { wrapper: Wrapper });

    act(() => {
      getGraphControlsProps()?.onToggleManualLayout();
    });

    expect(getRenderedEdgeById("edge-section-1-task-1-task-2")?.data?.isManualLayout).toBe(false);

    const onNodesChange = vi.mocked(ReactFlow).mock.lastCall?.[0]?.onNodesChange;

    act(() => {
      onNodesChange?.([
        { dragging: true, id: "section_1", position: changedGroupPosition, type: "position" },
      ]);
    });

    expect(getRenderedNodeById("section_1")?.position).toEqual(changedGroupPosition);
    expect(getRenderedNodeById("section_1.task_1")?.position).toEqual(mockGraphGroupChildNode.position);
    expect(getRenderedEdgeById("edge-section-1-task-1-task-2")?.data?.isManualLayout).toBe(true);

    act(() => {
      onNodesChange?.([
        { dragging: false, id: "section_1", position: changedGroupPosition, type: "position" },
      ]);
    });

    expect(getRenderedNodeById("section_1")?.position).toEqual(changedGroupPosition);
    expect(getRenderedNodeById("section_1.task_1")?.position).toEqual(mockGraphGroupChildNode.position);
  });

  it("keeps asset expression edges in ELK mode until an asset expression node moves", () => {
    vi.mocked(useGraphLayout).mockReturnValue({
      data: { edges: [mockAssetConditionEdge], nodes: [mockAssetConditionNode, mockGraphNode] },
      isPending: false,
    } as unknown as ReturnType<typeof useGraphLayout>);

    render(<Graph />, { wrapper: Wrapper });

    act(() => {
      getGraphControlsProps()?.onToggleManualLayout();
    });

    expect(getRenderedEdges()[0]?.data?.isManualLayout).toBe(false);

    const onNodesChange = vi.mocked(ReactFlow).mock.lastCall?.[0]?.onNodesChange;

    act(() => {
      onNodesChange?.([
        {
          dragging: true,
          id: "asset_condition_1",
          position: changedAssetConditionPosition,
          type: "position",
        },
      ]);
    });

    expect(getRenderedNodeById("asset_condition_1")?.position).toEqual(changedAssetConditionPosition);
    expect(getRenderedEdges()[0]?.data?.isManualLayout).toBe(true);
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

  it("allows overlapping nodes after release in manual mode", () => {
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

    expect(getRenderedNodeById("task_1")?.position).toEqual(overlappingNodePosition);

    act(() => {
      onNodesChange?.([
        { dragging: false, id: "task_1", position: overlappingNodePosition, type: "position" },
      ]);
    });

    expect(getRenderedNodeById("task_1")?.position).toEqual(overlappingNodePosition);
    expect(getRenderedNodeById("task_2")?.position).toEqual(mockGraphNodeTwo.position);
  });

  it("keeps manual layout mode after task selection remounts the graph route", () => {
    vi.useFakeTimers();

    const { unmount } = render(<Graph />, { wrapper: Wrapper });

    act(() => {
      getGraphControlsProps()?.onToggleManualLayout();
    });

    const onNodesChange = vi.mocked(ReactFlow).mock.lastCall?.[0]?.onNodesChange;

    act(() => {
      onNodesChange?.([{ dragging: false, id: "task_1", position: changedNodePosition, type: "position" }]);
    });

    act(() => {
      vi.mocked(ReactFlow).mock.lastCall?.[0]?.onViewportChange?.(changedViewport);
    });

    expect(vi.mocked(ReactFlow).mock.lastCall?.[0]?.nodesDraggable).toBe(true);
    expect(getRenderedNodeById("task_1")?.position).toEqual(changedNodePosition);

    unmount();
    scheduleClearManualLayoutStatesForDag("test_dag");
    mockParams = { dagId: "test_dag", taskId: "task_1" };

    const selectedRouteRender = render(<Graph />, { wrapper: Wrapper });

    act(() => {
      vi.runOnlyPendingTimers();
    });

    expect(vi.mocked(ReactFlow).mock.lastCall?.[0]?.nodesDraggable).toBe(true);
    expect(vi.mocked(ReactFlow).mock.lastCall?.[0]?.defaultViewport).toEqual(changedViewport);
    expect(getGraphControlsProps()?.isManualLayout).toBe(true);
    expect(getRenderedNodeById("task_1")?.position).toEqual(changedNodePosition);

    selectedRouteRender.unmount();
    scheduleClearManualLayoutStatesForDag("test_dag");
    mockParams = { dagId: "test_dag" };

    render(<Graph />, { wrapper: Wrapper });

    act(() => {
      vi.runOnlyPendingTimers();
    });

    expect(vi.mocked(ReactFlow).mock.lastCall?.[0]?.nodesDraggable).toBe(true);
    expect(vi.mocked(ReactFlow).mock.lastCall?.[0]?.defaultViewport).toEqual(changedViewport);
    expect(getGraphControlsProps()?.isManualLayout).toBe(true);
    expect(getRenderedNodeById("task_1")?.position).toEqual(changedNodePosition);

    vi.useRealTimers();
  });

  it("clears manual layout mode after leaving the Dag graph route", () => {
    vi.useFakeTimers();

    const { unmount } = render(<Graph />, { wrapper: Wrapper });

    act(() => {
      getGraphControlsProps()?.onToggleManualLayout();
    });

    const onNodesChange = vi.mocked(ReactFlow).mock.lastCall?.[0]?.onNodesChange;

    act(() => {
      onNodesChange?.([{ dragging: false, id: "task_1", position: changedNodePosition, type: "position" }]);
    });

    expect(vi.mocked(ReactFlow).mock.lastCall?.[0]?.nodesDraggable).toBe(true);

    unmount();
    scheduleClearManualLayoutStatesForDag("test_dag");

    act(() => {
      vi.runOnlyPendingTimers();
    });

    render(<Graph />, { wrapper: Wrapper });

    expect(vi.mocked(ReactFlow).mock.lastCall?.[0]?.nodesDraggable).toBe(false);
    expect(getGraphControlsProps()?.isManualLayout).toBe(false);
    expect(getRenderedNodeById("task_1")?.position).toEqual(mockGraphNode.position);

    vi.useRealTimers();
  });
});
