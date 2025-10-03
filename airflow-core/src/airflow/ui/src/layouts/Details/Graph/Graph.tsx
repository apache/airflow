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
import type { CSSProperties } from "react";
import { useEffect } from "react";
import { useParams } from "react-router-dom";
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
  const { dagId = "", runId = "", taskId } = useParams();

  const selectedVersion = useSelectedVersion();

  const [groupOdd, groupEven, selectedStroke, bg, pattern, controlsBg, controlsHover, minimapBg] = useToken("colors", [
    "graph.minimap.group.odd",
    "graph.minimap.group.even",
    "graph.selected.stroke",
    "graph.bg",
    "graph.pattern",
    "graph.controls.bg",
    "graph.controls.hover",
    "graph.minimap.bg",
  ]);

  const { allGroupIds, openGroupIds, setAllGroupIds } = useOpenGroups();

  const [dependencies] = useLocalStorage<"all" | "immediate" | "tasks">(`dependencies-${dagId}`, "tasks");
  const [direction] = useLocalStorage<Direction>(`direction-${dagId}`, "RIGHT");

  const { data: graphData = { edges: [], nodes: [] } } = useStructureServiceStructureData(
    {
      dagId,
      externalDependencies: dependencies === "immediate",
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
        isSelected: node.id === taskId || node.id === `dag:${dagId}`,
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
          edge.source === `dag:${dagId}` ||
          edge.target === `dag:${dagId}`,
      },
    },
  }));

  const reactFlowStyle: CSSProperties = {
    "--xy-background-color": bg,
    "--xy-background-pattern-color": pattern,
    "--xy-controls-button-background-color": controlsBg,
    "--xy-controls-button-background-color-hover": controlsHover,
    "--xy-minimap-background-color": minimapBg,
  } as CSSProperties;

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
      style={reactFlowStyle}
    >
      <Background />
      <Controls showInteractive={false} />
      <MiniMap
        nodeColor={(node: ReactFlowNode<CustomNodeProps>) =>
          nodeColor(node, groupEven, groupOdd)
        }
        nodeStrokeColor={(node: ReactFlowNode<CustomNodeProps>) =>
          node.data.isSelected && selectedStroke !== undefined ? selectedStroke : ""
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
