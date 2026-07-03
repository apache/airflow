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
import { Position } from "@xyflow/react";
import { beforeEach, describe, expect, it, vi } from "vitest";

import CustomEdge from "./Edge";
import type { EdgeData } from "./reactflowUtils";

const chakraMocks = vi.hoisted(() => ({
  useToken: vi.fn(() => ["#111111", "#222222", "#333333", "#444444", "#555555", "#666666"]),
}));
const xyFlowMocks = vi.hoisted(() => ({
  BaseEdge: vi.fn(() => null),
  useNodesData: vi.fn(() => [{ data: { isSelected: false } }, { data: { isSelected: false } }]),
  useStore: vi.fn(),
}));

vi.mock("@chakra-ui/react", async () => {
  const actual = await vi.importActual("@chakra-ui/react");

  return { ...actual, useToken: chakraMocks.useToken };
});

vi.mock("@xyflow/react", async () => {
  const actual = await vi.importActual("@xyflow/react");

  return {
    ...actual,
    BaseEdge: xyFlowMocks.BaseEdge,
    useNodesData: xyFlowMocks.useNodesData,
    useStore: xyFlowMocks.useStore,
  };
});

type MockNodeLookupEntry = {
  id: string;
  internals: {
    positionAbsolute: { x: number; y: number };
    userNode: {
      data: { height?: number; width?: number };
      dragging?: boolean;
      height?: number;
      width?: number;
    };
  };
  measured: { height?: number; width?: number };
};

type MockStoreState = {
  nodeLookup: Map<string, MockNodeLookupEntry>;
};

let nodeLookup = new Map<string, MockNodeLookupEntry>();

const buildMockNode = ({
  dragging = false,
  height = 80,
  id,
  position = { x: 0, y: 0 },
  width = 100,
}: {
  dragging?: boolean;
  height?: number;
  id: string;
  position?: { x: number; y: number };
  width?: number;
}): MockNodeLookupEntry => ({
  id,
  internals: {
    positionAbsolute: position,
    userNode: { data: { height, width }, dragging, height, width },
  },
  measured: { height, width },
});

const buildEdgeProps = ({
  source,
  sourceX = 0,
  sourceY = 0,
  target,
  targetX = 100,
  targetY = 100,
}: {
  source: string;
  sourceX?: number;
  sourceY?: number;
  target: string;
  targetX?: number;
  targetY?: number;
}): {
  data: EdgeData;
  id: string;
  source: string;
  sourcePosition: Position.Right;
  sourceX: number;
  sourceY: number;
  target: string;
  targetPosition: Position.Left;
  targetX: number;
  targetY: number;
  type: "custom";
} => ({
  data: {
    isManualLayout: true,
    rest: {
      id: `edge-${source}-${target}`,
      sources: [source],
      targets: [target],
    },
  },
  id: `edge-${source}-${target}`,
  source,
  sourcePosition: Position.Right,
  sourceX,
  sourceY,
  target,
  targetPosition: Position.Left,
  targetX,
  targetY,
  type: "custom",
});

const getLastBaseEdgeStroke = () => {
  const baseEdgeCalls = vi.mocked(xyFlowMocks.BaseEdge).mock.calls as unknown as Array<
    [{ style?: { stroke?: string | undefined } | undefined }]
  >;

  return baseEdgeCalls.at(-1)?.[0]?.style?.stroke;
};

const getLastBaseEdgePath = () => {
  const baseEdgeCalls = vi.mocked(xyFlowMocks.BaseEdge).mock.calls as unknown as Array<
    [{ path?: string | undefined }]
  >;

  return baseEdgeCalls.at(-1)?.[0]?.path;
};

describe("CustomEdge", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    nodeLookup = new Map([
      ["task_1", buildMockNode({ dragging: true, id: "task_1" })],
      ["task_2", buildMockNode({ id: "task_2", position: { x: 100, y: 100 } })],
      ["task_3", buildMockNode({ id: "task_3", position: { x: 0, y: 200 } })],
      ["task_4", buildMockNode({ id: "task_4", position: { x: 100, y: 200 } })],
    ]);

    vi.mocked(xyFlowMocks.useStore).mockImplementation((selector: (state: MockStoreState) => unknown) =>
      selector({ nodeLookup }),
    );
  });

  it("uses drag preview colors only for edges connected to dragged nodes", () => {
    render(<CustomEdge {...buildEdgeProps({ source: "task_1", target: "task_2" })} />);

    const connectedStroke = getLastBaseEdgeStroke();

    render(<CustomEdge {...buildEdgeProps({ source: "task_3", target: "task_4" })} />);

    const unconnectedStroke = getLastBaseEdgeStroke();

    expect(connectedStroke).toBe("#444444");
    expect(unconnectedStroke).toBe("#111111");
  });

  it("routes manual edges from side midpoints with a simple orthogonal path", () => {
    nodeLookup.set("task_2", buildMockNode({ id: "task_2", position: { x: 100, y: 100 } }));
    nodeLookup.set(
      "blocking_task",
      buildMockNode({ height: 60, id: "blocking_task", position: { x: 80, y: 20 }, width: 40 }),
    );

    render(
      <CustomEdge
        {...buildEdgeProps({
          source: "task_1",
          sourceX: 0,
          sourceY: 50,
          target: "task_2",
          targetX: 200,
          targetY: 50,
        })}
      />,
    );

    expect(getLastBaseEdgePath()).toBe("M 50 80 L 100 80 L 100 100 L 150 100");
  });

  it("updates manual edge attachment sides as nodes move", () => {
    nodeLookup.set("task_2", buildMockNode({ id: "task_2", position: { x: 240, y: 0 } }));

    render(
      <CustomEdge
        {...buildEdgeProps({
          source: "task_1",
          sourceX: 0,
          sourceY: 0,
          target: "task_2",
          targetX: 0,
          targetY: 0,
        })}
      />,
    );

    expect(getLastBaseEdgePath()).toBe("M 100 40 L 240 40");
  });
});
