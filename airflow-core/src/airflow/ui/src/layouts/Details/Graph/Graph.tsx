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
import { useParams } from "react-router-dom";
import { useLocalStorage } from "usehooks-ts";

import {
  useDagRunServiceGetDagRun,
  useGridServiceGridData,
  useStructureServiceStructureData,
} from "openapi/queries";
import { DownloadButton } from "src/components/Graph/DownloadButton";
import { edgeTypes, nodeTypes } from "src/components/Graph/graphTypes";
import type { CustomNodeProps } from "src/components/Graph/reactflowUtils";
import { type Direction, useGraphLayout } from "src/components/Graph/useGraphLayout";
import { useColorMode } from "src/context/colorMode";
import { useOpenGroups } from "src/context/openGroups";
import useSelectedVersion from "src/hooks/useSelectedVersion";
import { useDependencyGraph } from "src/queries/useDependencyGraph";
import { isStatePending, useAutoRefresh } from "src/utils";

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

  // corresponds to the "bg", "bg.emphasized", "border.inverted" semantic tokens
  const [oddLight, oddDark, evenLight, evenDark, selectedDarkColor, selectedLightColor] = useToken("colors", [
    "white",
    "black",
    "gray.200",
    "gray.800",
    "gray.200",
    "gray.800",
  ]);

  const { openGroupIds } = useOpenGroups();
  const refetchInterval = useAutoRefresh({ dagId });

  const [dependencies] = useLocalStorage<"all" | "immediate" | "tasks">(`dependencies-${dagId}`, "tasks");
  const [direction] = useLocalStorage<Direction>(`direction-${dagId}`, "RIGHT");

  const selectedColor = colorMode === "dark" ? selectedDarkColor : selectedLightColor;

  const { data: graphData = { edges: [], nodes: [] } } = useStructureServiceStructureData({
    dagId,
    externalDependencies: dependencies === "immediate",
    versionNumber: selectedVersion,
  });

  const { data: dagDependencies = { edges: [], nodes: [] } } = useDependencyGraph(`dag:${dagId}`, {
    enabled: dependencies === "all",
  });

  const { data: dagRun } = useDagRunServiceGetDagRun(
    {
      dagId,
      dagRunId: runId,
    },
    undefined,
    { enabled: runId !== "" },
  );

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

  // Filter grid data to get only a single dag run
  const { data: gridData } = useGridServiceGridData(
    {
      dagId,
      limit: 1,
      offset: 0,
      runAfterGte: dagRun?.run_after,
      runAfterLte: dagRun?.run_after,
    },
    undefined,
    {
      enabled: dagRun !== undefined,
      refetchInterval: (query) =>
        query.state.data?.dag_runs.some((dr) => isStatePending(dr.state)) && refetchInterval,
    },
  );

  const gridRun = gridData?.dag_runs.find((dr) => dr.dag_run_id === runId);

  // Add task instances to the node data but without having to recalculate how the graph is laid out
  const nodes = data?.nodes.map((node) => {
    const taskInstance = gridRun?.task_instances.find((ti) => ti.task_id === node.id);

    return {
      ...node,
      data: {
        ...node.data,
        isSelected: node.id === taskId,
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

  return (
    <ReactFlow
      colorMode={colorMode}
      defaultEdgeOptions={{ zIndex: 1 }}
      edges={edges}
      edgeTypes={edgeTypes}
      // Fit view to selected task or the whole graph on render
      fitView
      maxZoom={1}
      minZoom={0.25}
      nodes={nodes}
      nodesDraggable={false}
      nodeTypes={nodeTypes}
      onlyRenderVisibleElements
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
      <DownloadButton dagId={dagId} />
    </ReactFlow>
  );
};
