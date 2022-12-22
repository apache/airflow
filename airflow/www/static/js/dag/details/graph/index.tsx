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

import React, { useRef, useState, useEffect } from "react";
import { Box, useTheme, Select, Text } from "@chakra-ui/react";
import ReactFlow, {
  ReactFlowProvider,
  Controls,
  Background,
  MiniMap,
  Node as ReactFlowNode,
  useReactFlow,
  ControlButton,
  Panel,
} from "reactflow";
import { RiFocus3Line } from "react-icons/ri";

import { useGraphData, useGridData } from "src/api";
import useSelection from "src/dag/useSelection";
import { getTask, useOffsetTop } from "src/utils";
import type { TaskInstance } from "src/types";
import { useGraphLayout } from "src/utils/graph";
import type { NodeType } from "src/datasets/Graph/Node";
import Tooltip from "src/components/Tooltip";
import { useContainerRef } from "src/context/containerRef";

import Edge from "./Edge";
import Node, { CustomNodeProps } from "./Node";
import { buildEdges, nodeStrokeColor, nodeColor } from "./utils";

const nodeTypes = { custom: Node };
const edgeTypes = { custom: Edge };

// Child task positions are relative to their parent,
// we need to recursively added up the absolute position
const getNodePosition = (
  nodes: ReactFlowNode<CustomNodeProps>[],
  nodeId: string,
  x = 0,
  y = 0
) => {
  const node = nodes.find((n) => n.id === nodeId);
  if (node) {
    x += node.position.x;
    y += node.position.y;

    if (node?.parentNode) {
      const parentPosition = getNodePosition(nodes, node.parentNode);
      x += parentPosition.x;
      y += parentPosition.y;
    }
  }
  return { x, y };
};

interface Props {
  openGroupIds: string[];
  onToggleGroups: (groupIds: string[]) => void;
}

const Graph = ({ openGroupIds, onToggleGroups }: Props) => {
  const graphRef = useRef(null);
  const containerRef = useContainerRef();
  const { data } = useGraphData();
  const [arrange, setArrange] = useState(data?.arrange || "LR");

  useEffect(() => {
    setArrange(data?.arrange || "LR");
  }, [data?.arrange]);

  const { data: graphData } = useGraphLayout({
    edges: data?.edges,
    nodes: data?.nodes,
    openGroupIds,
    arrange,
  });
  const { selected } = useSelection();
  const {
    data: { dagRuns, groups },
  } = useGridData();
  const { colors } = useTheme();
  const { setCenter } = useReactFlow();
  const latestDagRunId = dagRuns[dagRuns.length - 1]?.runId;

  const offsetTop = useOffsetTop(graphRef);

  const nodes: ReactFlowNode<CustomNodeProps>[] = [];

  const flattenNodes = (children: NodeType[], parent?: string) => {
    const parentNode = parent ? { parentNode: parent } : undefined;
    children.forEach((node) => {
      let instance: TaskInstance | undefined;
      const group = getTask({ taskId: node.id, task: groups });
      if (!node.id.includes("join_id") && selected.runId) {
        instance = group?.instances.find((ti) => ti.runId === selected.runId);
      }
      const isSelected = node.id === selected.taskId && !!instance;

      nodes.push({
        id: node.id,
        data: {
          width: node.width,
          height: node.height,
          task: group,
          instance,
          isSelected,
          latestDagRunId,
          onToggleCollapse: () => {
            let newGroupIds = [];
            if (!node.value.isOpen) {
              newGroupIds = [...openGroupIds, node.value.label];
            } else {
              newGroupIds = openGroupIds.filter((g) => g !== node.value.label);
            }
            onToggleGroups(newGroupIds);
          },
          ...node.value,
        },
        type: "custom",
        position: {
          x: node.x || 0,
          y: node.y || 0,
        },
        ...parentNode,
      });
      if (node.children) {
        flattenNodes(node.children, node.id);
      }
    });
  };

  if (graphData?.children) {
    flattenNodes(graphData.children);
  }

  const focusNode = () => {
    if (selected.taskId) {
      const { x, y } = getNodePosition(nodes, selected.taskId);
      setCenter(x, y, { duration: 1000 });
    }
  };

  const edges = buildEdges({
    edges: graphData?.edges,
    nodes,
    selectedTaskId: selected.taskId,
  });

  return (
    <Box
      ref={graphRef}
      height={`calc(100% - ${offsetTop}px)`}
      borderWidth={1}
      borderColor="gray.200"
    >
      {!!offsetTop && (
        <ReactFlow
          nodes={nodes}
          edges={edges}
          nodesDraggable={false}
          nodeTypes={nodeTypes}
          edgeTypes={edgeTypes}
          minZoom={0.25}
          maxZoom={1}
          onlyRenderVisibleElements
        >
          <Panel position="top-right">
            <Box bg="#ffffffdd" p={1}>
              <Text>Layout:</Text>
              <Select
                value={arrange}
                onChange={(e) => setArrange(e.target.value)}
              >
                <option value="LR">Left -&gt; Right</option>
                <option value="RL">Right -&gt; Left</option>
                <option value="TB">Top -&gt; Bottom</option>
                <option value="BT">Bottom -&gt; Top</option>
              </Select>
            </Box>
          </Panel>
          <Background />
          <Controls showInteractive={false}>
            <ControlButton onClick={focusNode} disabled={!selected.taskId}>
              <Tooltip
                portalProps={{ containerRef }}
                label="Center selected task"
                placement="right"
              >
                <Box>
                  <RiFocus3Line
                    size={16}
                    style={{
                      // override react-flow css
                      maxWidth: "16px",
                      maxHeight: "16px",
                      color: colors.gray[800],
                    }}
                    aria-label="Center selected task"
                  />
                </Box>
              </Tooltip>
            </ControlButton>
          </Controls>
          <MiniMap
            nodeStrokeWidth={15}
            nodeStrokeColor={(props) => nodeStrokeColor(props, colors)}
            nodeColor={nodeColor}
            zoomable
            pannable
          />
        </ReactFlow>
      )}
    </Box>
  );
};

const GraphWrapper = ({ openGroupIds, onToggleGroups }: Props) => (
  <ReactFlowProvider>
    <Graph openGroupIds={openGroupIds} onToggleGroups={onToggleGroups} />
  </ReactFlowProvider>
);

export default GraphWrapper;
