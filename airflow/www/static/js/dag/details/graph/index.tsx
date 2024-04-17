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
  useOnViewportChange,
  Viewport,
  ControlButton,
} from "reactflow";
import { BiCollapse, BiExpand } from "react-icons/bi";

import { useDatasets, useGraphData, useGridData } from "src/api";
import useSelection from "src/dag/useSelection";
import { getMetaValue, getTask, useOffsetTop } from "src/utils";
import { useGraphLayout } from "src/utils/graph";
import Edge from "src/components/Graph/Edge";
import type { DepNode, WebserverEdge } from "src/types";

import Node from "./Node";
import { buildEdges, nodeStrokeColor, nodeColor, flattenNodes } from "./utils";

const nodeTypes = { custom: Node };
const edgeTypes = { custom: Edge };

interface Props {
  openGroupIds: string[];
  onToggleGroups: (groupIds: string[]) => void;
  hoveredTaskState?: string | null;
  isFullScreen?: boolean;
  toggleFullScreen?: () => void;
}

const dagId = getMetaValue("dag_id");

const Graph = ({
  openGroupIds,
  onToggleGroups,
  hoveredTaskState,
  isFullScreen,
  toggleFullScreen,
}: Props) => {
  const graphRef = useRef(null);
  const { data } = useGraphData();
  const [arrange, setArrange] = useState(data?.arrange || "LR");
  const [hasRendered, setHasRendered] = useState(false);
  const [isZoomedOut, setIsZoomedOut] = useState(false);

  const {
    data: { dagRuns, groups },
  } = useGridData();

  useEffect(() => {
    setArrange(data?.arrange || "LR");
  }, [data?.arrange]);

  const { data: datasetsCollection } = useDatasets({
    dagIds: [dagId],
  });

  const rawNodes =
    data?.nodes && datasetsCollection?.datasets?.length
      ? {
          ...data.nodes,
          children: [
            ...(data.nodes.children || []),
            ...(datasetsCollection?.datasets || []).map(
              (dataset) =>
                ({
                  id: dataset?.id?.toString() || "",
                  value: {
                    class: "dataset",
                    label: dataset.uri,
                  },
                } as DepNode)
            ),
          ],
        }
      : data?.nodes;

  const datasetEdges: WebserverEdge[] = [];

  datasetsCollection?.datasets?.forEach((dataset) => {
    const producingTask = dataset?.producingTasks?.find(
      (t) => t.dagId === dagId
    );
    const consumingDag = dataset?.consumingDags?.find((d) => d.dagId === dagId);
    if (dataset.id) {
      // check that the task is in the graph
      if (
        producingTask?.taskId &&
        getTask({ taskId: producingTask?.taskId, task: groups })
      ) {
        datasetEdges.push({
          sourceId: producingTask.taskId,
          targetId: dataset.id.toString(),
        });
      }
      if (consumingDag && data?.nodes?.children?.length) {
        datasetEdges.push({
          sourceId: dataset.id.toString(),
          // Point upstream datasets to the first task
          targetId: data.nodes?.children[0].id,
          isSourceDataset: true,
        });
      }
    }
  });

  const { data: graphData } = useGraphLayout({
    edges: [...(data?.edges || []), ...datasetEdges],
    nodes: rawNodes,
    openGroupIds,
    arrange,
  });

  const { selected } = useSelection();

  const { colors } = useTheme();
  const { getZoom, fitView } = useReactFlow();
  const latestDagRunId = dagRuns[dagRuns.length - 1]?.runId;
  const offsetTop = useOffsetTop(graphRef);

  useOnViewportChange({
    onEnd: (viewport: Viewport) => {
      if (viewport.zoom < 0.5 && !isZoomedOut) setIsZoomedOut(true);
      if (viewport.zoom >= 0.5 && isZoomedOut) setIsZoomedOut(false);
    },
  });

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
        isZoomedOut,
      }),
    [
      graphData?.children,
      selected,
      openGroupIds,
      onToggleGroups,
      latestDagRunId,
      groups,
      hoveredTaskState,
      isZoomedOut,
    ]
  );

  // Zoom to/from nodes when changing selection, maintain zoom level when changing task selection
  useEffect(() => {
    if (hasRendered) {
      const zoom = getZoom();
      if (zoom < 0.5) setIsZoomedOut(true);
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
    isZoomedOut,
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
          <Controls showInteractive={false}>
            <ControlButton
              onClick={toggleFullScreen}
              aria-label="Toggle full screen"
              title="Toggle full screen"
            >
              {isFullScreen ? <BiCollapse /> : <BiExpand />}
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

const GraphWrapper = (props: Props) => (
  <ReactFlowProvider>
    <Graph {...props} />
  </ReactFlowProvider>
);

export default GraphWrapper;
