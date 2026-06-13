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
  getSmoothStepPath: vi.fn(() => ["M 0 0"]),
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
    getSmoothStepPath: xyFlowMocks.getSmoothStepPath,
    useNodesData: xyFlowMocks.useNodesData,
    useStore: xyFlowMocks.useStore,
  };
});

type MockNodeLookupEntry = {
  internals: { userNode: { dragging?: boolean } };
};

type MockStoreState = {
  nodeLookup: Map<string, MockNodeLookupEntry>;
};

let nodeLookup = new Map<string, MockNodeLookupEntry>();

const buildEdgeProps = ({
  source,
  target,
}: {
  source: string;
  target: string;
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
  sourceX: 0,
  sourceY: 0,
  target,
  targetPosition: Position.Left,
  targetX: 100,
  targetY: 100,
  type: "custom",
});

const getLastBaseEdgeStroke = () => {
  const baseEdgeCalls = vi.mocked(xyFlowMocks.BaseEdge).mock.calls as unknown as Array<
    [{ style?: { stroke?: string | undefined } | undefined }]
  >;

  return baseEdgeCalls.at(-1)?.[0]?.style?.stroke;
};

describe("CustomEdge", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    nodeLookup = new Map([
      ["task_1", { internals: { userNode: { dragging: true } } }],
      ["task_2", { internals: { userNode: { dragging: false } } }],
      ["task_3", { internals: { userNode: { dragging: false } } }],
      ["task_4", { internals: { userNode: { dragging: false } } }],
    ]);

    vi.mocked(xyFlowMocks.useStore).mockImplementation((selector: (state: MockStoreState) => boolean) =>
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
});
