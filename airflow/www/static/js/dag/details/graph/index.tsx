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

import React, { useRef, useState, useEffect, useMemo } from "react";
import { Box, useTheme, Select, Text } from "@chakra-ui/react";
import ReactFlow, {
  ReactFlowProvider,
  Controls,
  Background,
  MiniMap,
  useReactFlow,
  Panel,
} from "reactflow";

import { useGraphData, useGridData } from "src/api";
import useSelection from "src/dag/useSelection";
import { useOffsetTop } from "src/utils";
import { useGraphLayout } from "src/utils/graph";
import Edge from "src/components/Graph/Edge";

import Node from "./Node";
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
  const { data } = useGraphData();
  const [arrange, setArrange] = useState(data?.arrange || "LR");
  const [hasRendered, setHasRendered] = useState(false);

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
  const { getZoom, fitView } = useReactFlow();
  const latestDagRunId = dagRuns[dagRuns.length - 1]?.runId;
  const offsetTop = useOffsetTop(graphRef);

  const { nodes, edges: nodeEdges } = useMemo(
    () =>
      flattenNodes({
        children: graphData?.children,
        selected,
        openGroupIds,
        onToggleGroups,
        latestDagRunId,
        groups,
        hoveredTaskState,
      }),
    [
      graphData?.children,
      selected,
      openGroupIds,
      onToggleGroups,
      latestDagRunId,
      groups,
      hoveredTaskState,
    ]
  );

  // Zoom to/from nodes when changing selection, maintain zoom level when changing task selection
  useEffect(() => {
    if (hasRendered) {
      const zoom = getZoom();
      fitView({
        duration: 750,
        nodes: selected.taskId ? [{ id: selected.taskId }] : undefined,
        minZoom: selected.taskId ? zoom : undefined,
        maxZoom: selected.taskId ? zoom : undefined,
      });
    }
    setHasRendered(true);
  }, [fitView, hasRendered, selected.taskId, getZoom]);

  // merge & dedupe edges
  const flatEdges = [...(graphData?.edges || []), ...(nodeEdges || [])].filter(
    (value, index, self) => index === self.findIndex((t) => t.id === value.id)
  );

  const edges = buildEdges({
    edges: flatEdges,
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
          // Fit view to selected task or the whole graph on render
          fitView
          fitViewOptions={{
            nodes: selected.taskId ? [{ id: selected.taskId }] : undefined,
          }}
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
          <Controls showInteractive={false} />
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
