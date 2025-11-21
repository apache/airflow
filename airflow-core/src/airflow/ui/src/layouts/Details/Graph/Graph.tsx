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
import { useToken } from "@chakra-ui/react";
import { ReactFlow, Controls, Background, MiniMap, type Node as ReactFlowNode } from "@xyflow/react";
import "@xyflow/react/dist/style.css";
import { useEffect } from "react";
import { useParams, useSearchParams } from "react-router-dom";
import { useLocalStorage } from "usehooks-ts";

import { useStructureServiceStructureData } from "openapi/queries";
import { DownloadButton } from "src/components/Graph/DownloadButton";
import { edgeTypes, nodeTypes } from "src/components/Graph/graphTypes";
import type { CustomNodeProps } from "src/components/Graph/reactflowUtils";
import { type Direction, useGraphLayout } from "src/components/Graph/useGraphLayout";
import { useColorMode } from "src/context/colorMode";
import { useOpenGroups } from "src/context/openGroups";
import useSelectedVersion from "src/hooks/useSelectedVersion";
import { flattenGraphNodes } from "src/layouts/Details/Grid/utils.ts";
import { useDependencyGraph } from "src/queries/useDependencyGraph";
import { useGridTiSummaries } from "src/queries/useGridTISummaries.ts";
import { getReactFlowThemeStyle } from "src/theme";

const nodeColor = (
  { data: { depth, height, isOpen, taskInstance, width }, type }: ReactFlowNode<CustomNodeProps>,
  evenColor?: string,
  oddColor?: string,
) => {
  if (height === undefined || width === undefined || type === "join") {
    return "";
  }

  if (taskInstance?.state !== undefined && !isOpen) {
    return `var(--chakra-colors-${taskInstance.state}-solid)`;
  }

  if (isOpen && depth !== undefined && depth % 2 === 0) {
    return evenColor ?? "";
  } else if (isOpen) {
    return oddColor ?? "";
  }

  return "";
};

export const Graph = () => {
  const { colorMode = "light" } = useColorMode();
  const { dagId = "", groupId, runId = "", taskId } = useParams();
  const [searchParams] = useSearchParams();

  const selectedVersion = useSelectedVersion();

  const filterRoot = searchParams.get("root") ?? undefined;
  const includeUpstream = searchParams.get("upstream") === "true";
  const includeDownstream = searchParams.get("downstream") === "true";

  const hasActiveFilter = includeUpstream || includeDownstream;

  // corresponds to the "bg", "bg.emphasized", "border.inverted" semantic tokens
  const [oddLight, oddDark, evenLight, evenDark, selectedDarkColor, selectedLightColor] = useToken("colors", [
    "bg",
    "fg",
    "bg.muted",
    "bg.emphasized",
    "bg.muted",
    "bg.emphasized",
  ]);

  const { allGroupIds, openGroupIds, setAllGroupIds } = useOpenGroups();

  const [dependencies] = useLocalStorage<"all" | "immediate" | "tasks">(`dependencies-${dagId}`, "tasks");
  const [direction] = useLocalStorage<Direction>(`direction-${dagId}`, "RIGHT");

  const selectedColor = colorMode === "dark" ? selectedDarkColor : selectedLightColor;
  const { data: graphData = { edges: [], nodes: [] } } = useStructureServiceStructureData(
    {
      dagId,
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

  const { data } = useGraphLayout({
    direction,
    edges: [...graphData.edges, ...dagDepEdges],
    nodes: dagDepNodes.length
      ? dagDepNodes.map((node) =>
          node.id === `dag:${dagId}` ? { ...node, children: graphData.nodes } : node,
        )
      : graphData.nodes,
    openGroupIds: [...openGroupIds, ...(dependencies === "all" ? [`dag:${dagId}`] : [])],
    versionNumber: selectedVersion,
  });

  const { data: gridTISummaries } = useGridTiSummaries({ dagId, runId });

  // Add task instances to the node data but without having to recalculate how the graph is laid out
  const nodes = data?.nodes.map((node) => {
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

  const edges = (data?.edges ?? []).map((edge) => ({
    ...edge,
    data: {
      ...edge.data,
      rest: {
        ...edge.data?.rest,
        isSelected:
          taskId === edge.source ||
          taskId === edge.target ||
          groupId === edge.source ||
          groupId === edge.target ||
          edge.source === `dag:${dagId}` ||
          edge.target === `dag:${dagId}`,
      },
    },
  }));

  return (
    <ReactFlow
      colorMode={colorMode}
      defaultEdgeOptions={{ zIndex: 1 }}
      edges={edges}
      edgeTypes={edgeTypes}
      // Fit view to selected task or the whole graph on render
      fitView
      maxZoom={1.5}
      minZoom={0.25}
      nodes={nodes}
      nodesDraggable={false}
      nodeTypes={nodeTypes}
      onlyRenderVisibleElements
      style={getReactFlowThemeStyle(colorMode)}
    >
      <Background />
      <Controls showInteractive={false} />
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
      <DownloadButton name={dagId} />
    </ReactFlow>
  );
};
