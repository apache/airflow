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
import {
  applyNodeChanges,
  Background,
  MiniMap,
  ReactFlow,
  type Node as ReactFlowNode,
  type NodeChange,
} from "@xyflow/react";
import "@xyflow/react/dist/style.css";
import { useEffect, useMemo, useState } from "react";
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
const defaultEdgeOptions = {
  interactionWidth: 0,
  zIndex: -1,
};

const collisionPadding = 12;
const searchGridSize = 24;
const maxSearchRadius = 24;
const fallbackNodeWidth = 100;
const fallbackNodeHeight = 64;
const radialDirections = [
  { x: 1, y: 0 },
  { x: -1, y: 0 },
  { x: 0, y: 1 },
  { x: 0, y: -1 },
  { x: 1, y: 1 },
  { x: 1, y: -1 },
  { x: -1, y: 1 },
  { x: -1, y: -1 },
];

const getNodeDimensions = (node: ReactFlowNode<CustomNodeProps>) => ({
  height: node.height ?? node.data.height ?? fallbackNodeHeight,
  width: node.width ?? node.data.width ?? fallbackNodeWidth,
});

const nodesOverlap = (
  firstNode: ReactFlowNode<CustomNodeProps>,
  secondNode: ReactFlowNode<CustomNodeProps>,
  firstPosition = firstNode.position,
) => {
  const firstDimensions = getNodeDimensions(firstNode);
  const secondDimensions = getNodeDimensions(secondNode);

  return (
    firstPosition.x < secondNode.position.x + secondDimensions.width + collisionPadding &&
    firstPosition.x + firstDimensions.width + collisionPadding > secondNode.position.x &&
    firstPosition.y < secondNode.position.y + secondDimensions.height + collisionPadding &&
    firstPosition.y + firstDimensions.height + collisionPadding > secondNode.position.y
  );
};

const findNearestOpenPosition = ({
  movedNode,
  nodes,
}: {
  movedNode: ReactFlowNode<CustomNodeProps>;
  nodes: Array<ReactFlowNode<CustomNodeProps>>;
}) => {
  const otherNodes = nodes.filter((node) => node.id !== movedNode.id);

  const isPositionFree = (position: { x: number; y: number }) =>
    otherNodes.every((node) => !nodesOverlap(movedNode, node, position));

  if (isPositionFree(movedNode.position)) {
    return movedNode.position;
  }

  for (let radius = 1; radius <= maxSearchRadius; radius += 1) {
    for (const direction of radialDirections) {
      const position = {
        x: movedNode.position.x + direction.x * radius * searchGridSize,
        y: movedNode.position.y + direction.y * radius * searchGridSize,
      };

      if (isPositionFree(position)) {
        return position;
      }
    }
  }

  return movedNode.position;
};

const avoidNodeOverlap = ({
  changes,
  nodes,
}: {
  changes: Array<NodeChange<ReactFlowNode<CustomNodeProps>>>;
  nodes: Array<ReactFlowNode<CustomNodeProps>>;
}) => {
  const movedNodeIds = new Set(
    changes
      .filter(
        (
          change,
        ): change is {
          dragging: false;
          id: string;
          type: "position";
        } & NodeChange<ReactFlowNode<CustomNodeProps>> =>
          change.type === "position" && change.dragging === false,
      )
      .map((change) => change.id),
  );

  if (movedNodeIds.size === 0) {
    return nodes;
  }

  const resolvedNodes = [...nodes];

  for (const movedNodeId of movedNodeIds) {
    const movedNodeIndex = resolvedNodes.findIndex((node) => node.id === movedNodeId);
    const movedNode = movedNodeIndex === -1 ? undefined : resolvedNodes[movedNodeIndex];

    if (movedNode !== undefined) {
      const position = findNearestOpenPosition({ movedNode, nodes: resolvedNodes });

      if (position !== movedNode.position) {
        resolvedNodes[movedNodeIndex] = { ...movedNode, position };
      }
    }
  }

  return resolvedNodes;
};

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
  const [isManualLayout, setIsManualLayout] = useState(false);
  const [manualNodes, setManualNodes] = useState<Array<ReactFlowNode<CustomNodeProps>>>([]);

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

  // Add task instances to the node data but without having to recalculate how the graph is laid out.
  // Keep the mapped array stable while inputs are unchanged so manual-layout state sync does not
  // retrigger itself in a render loop.
  const nodesWithTI = useMemo(
    () =>
      data?.nodes.map((node) => {
        const taskInstance = gridTISummaries?.task_instances.find((ti) => ti.task_id === node.id);

        return {
          ...node,
          data: {
            ...node.data,
            isSelected: node.id === taskId || node.id === groupId || node.id === `dag:${dagId}`,
            taskInstance,
          },
        };
      }),
    [dagId, data?.nodes, gridTISummaries, groupId, taskId],
  );

  const baseFilteredNodes = useGraphFilteredNodes(nodesWithTI, graphFilters);

  const { edges, nodes } = useFilteredNodesAndEdges({
    baseFilteredNodes,
    dagId,
    groupId,
    layoutEdges: data?.edges ?? [],
    taskId,
  });

  useEffect(() => {
    if (!isManualLayout) {
      return;
    }

    setManualNodes((currentNodes) => {
      if (nodes === undefined) {
        return [];
      }

      if (currentNodes.length === 0) {
        return nodes;
      }

      const currentPositionsById = new Map(currentNodes.map((node) => [node.id, node.position]));

      return nodes.map((node) => {
        const position = currentPositionsById.get(node.id);

        return position === undefined ? node : { ...node, position };
      });
    });
  }, [isManualLayout, nodes]);

  const onNodesChange = (changes: Array<NodeChange<ReactFlowNode<CustomNodeProps>>>) => {
    if (!isManualLayout) {
      return;
    }

    setManualNodes((currentNodes) => {
      const changedNodes = applyNodeChanges(changes, currentNodes);

      return avoidNodeOverlap({ changes, nodes: changedNodes });
    });
  };

  const toggleManualLayout = () => {
    setIsManualLayout((currentValue) => {
      const nextValue = !currentValue;

      setManualNodes(nextValue ? (nodes ?? []) : []);

      return nextValue;
    });
  };

  const nodesToRender = isManualLayout ? manualNodes : (nodes ?? []);
  const edgesToRender = useMemo(
    () =>
      edges.map((edge) => ({
        ...edge,
        data: {
          ...edge.data,
          isManualLayout,
        },
      })),
    [edges, isManualLayout],
  );
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
        edges={edgesToRender}
        edgesFocusable={false}
        edgeTypes={edgeTypes}
        maxZoom={1.5}
        minZoom={0.01}
        nodes={nodesToRender}
        nodesConnectable={false}
        nodesDraggable={isManualLayout}
        nodesFocusable={false}
        nodeTypes={nodeTypes}
        onlyRenderVisibleElements
        onNodesChange={onNodesChange}
        style={getReactFlowThemeStyle(colorMode)}
      >
        <Background />
        {/* Fit the viewport after each new ELK layout instead of using the
            fitView prop, which re-fires on every re-mount even when nodes are
            served from the React Query cache. */}
        <FitViewOnLayout layoutData={isManualLayout ? undefined : data} />
        <GraphControls
          isManualLayout={isManualLayout}
          onToggleManualLayout={toggleManualLayout}
          selectedNodeId={selectedNodeId}
        />
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
