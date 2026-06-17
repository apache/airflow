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
import { Box, Spinner, useToken } from "@chakra-ui/react";
import { ReactFlow, Background, MiniMap, type Node as ReactFlowNode } from "@xyflow/react";
import "@xyflow/react/dist/style.css";
import { useEffect } from "react";
import { useParams } from "react-router-dom";
import { useLocalStorage } from "usehooks-ts";

import { useDagRunServiceGetDagRun, useStructureServiceStructureData } from "openapi/queries";
import type { Direction } from "src/components/Graph/DirectionDropdown";
import { DownloadButton } from "src/components/Graph/DownloadButton";
import { edgeTypes, nodeTypes } from "src/components/Graph/graphTypes";
import type { CustomNodeProps } from "src/components/Graph/reactflowUtils";
import { useGraphLayout } from "src/components/Graph/useGraphLayout";
import { dependenciesKey, directionKey } from "src/constants/localStorage";
import { useColorMode } from "src/context/colorMode";
import { useGroups } from "src/context/groups";
import useSelectedVersion from "src/hooks/useSelectedVersion";
import { flattenGraphNodes } from "src/layouts/Details/Grid/utils.ts";
import { useDependencyGraph } from "src/queries/useDependencyGraph";
import { useGridTiSummariesStream } from "src/queries/useGridTISummaries.ts";
import { getReactFlowThemeStyle } from "src/theme";

import { FitViewOnLayout } from "./components/FitViewOnLayout";
import { GraphControls } from "./components/GraphControls";
import { useFilteredNodesAndEdges } from "./hooks/useFilteredNodesAndEdges";
import { useGraphSearchParams } from "./hooks/useGraphSearchParams";
import { useGraphFilteredNodes } from "./useGraphFilteredNodes";
import { nodeColor } from "./utils/nodeColor";

// Hoisted to module scope so ReactFlow receives a stable reference and skips
// its internal shallow-equality check on every render.
const defaultEdgeOptions = { zIndex: 1 };

export const Graph = () => {
  const { colorMode = "light" } = useColorMode();
  const { dagId = "", groupId, runId = "", taskId } = useParams();

  const selectedVersion = useSelectedVersion();

  const { depth, filterRoot, graphFilters, hasActiveFilter, includeDownstream, includeUpstream } =
    useGraphSearchParams();

  // corresponds to the "bg", "bg.emphasized", "border.inverted" semantic tokens
  const [oddLight, oddDark, evenLight, evenDark, selectedDarkColor, selectedLightColor] = useToken("colors", [
    "bg",
    "fg",
    "bg.muted",
    "bg.emphasized",
    "bg.muted",
    "bg.emphasized",
  ]);

  const { allGroupIds, openGroupIds, setAllGroupIds } = useGroups();

  const [dependencies] = useLocalStorage<"all" | "immediate" | "tasks">(dependenciesKey(dagId), "tasks");
  const [direction] = useLocalStorage<Direction>(directionKey(dagId), "RIGHT");

  const selectedColor = colorMode === "dark" ? selectedDarkColor : selectedLightColor;
  const { data: graphData = { edges: [], nodes: [] } } = useStructureServiceStructureData(
    {
      dagId,
      depth,
      externalDependencies: dependencies === "immediate",
      includeDownstream,
      includeUpstream,
      root: hasActiveFilter && filterRoot !== undefined ? filterRoot : undefined,
      versionNumber: selectedVersion,
    },
    undefined,
    { enabled: selectedVersion !== undefined },
  );

  useEffect(() => {
    const observedGroupIds = flattenGraphNodes(graphData.nodes).allGroupIds;

    if (
      observedGroupIds.length !== allGroupIds.length ||
      observedGroupIds.some((element, index) => allGroupIds[index] !== element)
    ) {
      setAllGroupIds(observedGroupIds);
    }
  }, [allGroupIds, graphData.nodes, setAllGroupIds]);

  const { data: dagDependencies = { edges: [], nodes: [] } } = useDependencyGraph(`dag:${dagId}`, {
    enabled: dependencies === "all",
  });

  const dagDepEdges = dependencies === "all" ? dagDependencies.edges : [];
  const dagDepNodes = dependencies === "all" ? dagDependencies.nodes : [];

  const layoutEdges = [...graphData.edges, ...dagDepEdges];
  const layoutNodes = dagDepNodes.length
    ? dagDepNodes.map((node) => (node.id === `dag:${dagId}` ? { ...node, children: graphData.nodes } : node))
    : graphData.nodes;
  const layoutOpenGroupIds = [...openGroupIds, ...(dependencies === "all" ? [`dag:${dagId}`] : [])];

  const { data, isPending } = useGraphLayout({
    direction,
    edges: layoutEdges,
    nodes: layoutNodes,
    openGroupIds: layoutOpenGroupIds,
    versionNumber: selectedVersion,
  });

  const { data: dagRun } = useDagRunServiceGetDagRun({ dagId, dagRunId: runId }, undefined, {
    enabled: Boolean(runId),
  });

  const { summariesByRunId } = useGridTiSummariesStream({
    dagId,
    runIds: runId ? [runId] : [],
    states: dagRun ? [dagRun.state] : undefined,
  });
  const gridTISummaries = runId ? summariesByRunId.get(runId) : undefined;

  // Add task instances to the node data but without having to recalculate how the graph is laid out
  const nodesWithTI = data?.nodes.map((node) => {
    const taskInstance = gridTISummaries?.task_instances.find((ti) => ti.task_id === node.id);

    return {
      ...node,
      data: {
        ...node.data,
        isSelected: node.id === taskId || node.id === groupId || node.id === `dag:${dagId}`,
        taskInstance,
      },
    };
  });

  const baseFilteredNodes = useGraphFilteredNodes(nodesWithTI, graphFilters);

  const { edges, nodes } = useFilteredNodesAndEdges({
    baseFilteredNodes,
    dagId,
    groupId,
    layoutEdges: data?.edges ?? [],
    taskId,
  });

  const selectedNodeId = taskId ?? groupId;

  return (
    <Box height="100%" position="relative" width="100%">
      {isPending ? (
        <Box
          alignItems="center"
          display="flex"
          height="100%"
          justifyContent="center"
          left={0}
          position="absolute"
          top={0}
          width="100%"
          zIndex={10}
        >
          <Spinner color="blue.500" size="xl" />
        </Box>
      ) : undefined}
      <ReactFlow
        colorMode={colorMode}
        defaultEdgeOptions={defaultEdgeOptions}
        edges={edges}
        edgesFocusable={false}
        edgeTypes={edgeTypes}
        maxZoom={1.5}
        minZoom={0.01}
        nodes={nodes}
        nodesConnectable={false}
        nodesDraggable={false}
        nodesFocusable={false}
        nodeTypes={nodeTypes}
        onlyRenderVisibleElements
        style={getReactFlowThemeStyle(colorMode)}
      >
        <Background />
        {/* Fit the viewport after each new ELK layout instead of using the
            fitView prop, which re-fires on every re-mount even when nodes are
            served from the React Query cache. */}
        <FitViewOnLayout layoutData={data} />
        <GraphControls selectedNodeId={selectedNodeId} />
        {/* Hide the MiniMap for large graphs — it processes all nodes even when
            onlyRenderVisibleElements is set, adding meaningful paint cost with
            little benefit at 500+ nodes where the map is a near-solid blob. */}
        {data !== undefined && data.nodes.length <= 500 ? (
          <MiniMap
            nodeColor={(node: ReactFlowNode<CustomNodeProps>) =>
              nodeColor(
                node,
                colorMode === "dark" ? evenDark : evenLight,
                colorMode === "dark" ? oddDark : oddLight,
              )
            }
            nodeStrokeColor={(node: ReactFlowNode<CustomNodeProps>) =>
              node.data.isSelected && selectedColor !== undefined ? selectedColor : ""
            }
            nodeStrokeWidth={15}
            pannable
            style={{ height: 150, width: 200 }}
            zoomable
          />
        ) : undefined}
        <DownloadButton name={dagId} />
      </ReactFlow>
    </Box>
  );
};
