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
import { useOffsetTop } from "src/utils";
import { useGraphLayout } from "src/utils/graph";
import Tooltip from "src/components/Tooltip";
import { useContainerRef } from "src/context/containerRef";
import useFilters from "src/dag/useFilters";

import Edge from "./Edge";
import Node, { CustomNodeProps } from "./Node";
import { buildEdges, nodeStrokeColor, nodeColor, flattenNodes } from "./utils";

const nodeTypes = { custom: Node };
const edgeTypes = { custom: Edge };

interface Props {
  openGroupIds: string[];
  onToggleGroups: (groupIds: string[]) => void;
  hoveredTaskState?: string | null;
}

const Graph = ({ openGroupIds, onToggleGroups, hoveredTaskState }: Props) => {
  const graphRef = useRef(null);
  const containerRef = useContainerRef();
  const { data } = useGraphData();
  const [arrange, setArrange] = useState(data?.arrange || "LR");

  const {
    filters: { root, filterDownstream, filterUpstream },
  } = useFilters();

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
  const { setCenter, setViewport } = useReactFlow();
  const latestDagRunId = dagRuns[dagRuns.length - 1]?.runId;

  // Reset viewport when tasks are filtered
  useEffect(() => {
    setViewport({ x: 0, y: 0, zoom: 1 });
  }, [root, filterDownstream, filterUpstream, setViewport]);

  const offsetTop = useOffsetTop(graphRef);

  let nodes: ReactFlowNode<CustomNodeProps>[] = [];

  if (graphData?.children) {
    nodes = flattenNodes({
      children: graphData.children,
      selected,
      openGroupIds,
      onToggleGroups,
      latestDagRunId,
      groups,
      hoveredTaskState,
    });
  }

  const focusNode = () => {
    if (selected.taskId) {
      const node = nodes.find((n) => n.id === selected.taskId);
      const x = node?.positionAbsolute?.x || node?.position.x;
      const y = node?.positionAbsolute?.y || node?.position.y;
      if (!x || !y) return;
      setCenter(
        x + (node.data.width || 0) / 2,
        y + (node.data.height || 0) / 2,
        {
          duration: 1000,
        }
      );
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
          defaultEdgeOptions={{ zIndex: 1 }}
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

const GraphWrapper = ({
  openGroupIds,
  onToggleGroups,
  hoveredTaskState,
}: Props) => (
  <ReactFlowProvider>
    <Graph
      openGroupIds={openGroupIds}
      onToggleGroups={onToggleGroups}
      hoveredTaskState={hoveredTaskState}
    />
  </ReactFlowProvider>
);

export default GraphWrapper;
