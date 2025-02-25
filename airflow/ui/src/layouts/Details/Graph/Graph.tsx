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
import { Flex, useToken } from "@chakra-ui/react";
import { ReactFlow, Controls, Background, MiniMap, type Node as ReactFlowNode } from "@xyflow/react";
import "@xyflow/react/dist/style.css";
import { useParams } from "react-router-dom";

import { useGridServiceGridData, useStructureServiceStructureData } from "openapi/queries";
import { useColorMode } from "src/context/colorMode";
import { useOpenGroups } from "src/context/openGroups";
import { isStatePending, useAutoRefresh } from "src/utils";

import Edge from "./Edge";
import { JoinNode } from "./JoinNode";
import { TaskNode } from "./TaskNode";
import type { CustomNodeProps } from "./reactflowUtils";
import { useGraphLayout } from "./useGraphLayout";

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

const nodeTypes = {
  join: JoinNode,
  task: TaskNode,
};
const edgeTypes = { custom: Edge };

export const Graph = () => {
  const { colorMode = "light" } = useColorMode();
  const { dagId = "", runId, taskId } = useParams();

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

  const selectedColor = colorMode === "dark" ? selectedDarkColor : selectedLightColor;

  const { data: graphData = { arrange: "LR", edges: [], nodes: [] } } = useStructureServiceStructureData({
    dagId,
  });

  const { data } = useGraphLayout({
    ...graphData,
    dagId,
    openGroupIds,
  });

  const refetchInterval = useAutoRefresh({ dagId });

  const { data: gridData } = useGridServiceGridData(
    {
      dagId,
      limit: 25,
      offset: 0,
      orderBy: "-run_after",
    },
    undefined,
    {
      enabled: Boolean(runId),
      refetchInterval: (query) =>
        query.state.data?.dag_runs.some((dr) => isStatePending(dr.state)) && refetchInterval,
    },
  );

  const dagRun = gridData?.dag_runs.find((dr) => dr.dag_run_id === runId);

  // Add task instances to the node data but without having to recalculate how the graph is laid out
  const nodes =
    dagRun?.task_instances === undefined
      ? data?.nodes
      : data?.nodes.map((node) => {
          const taskInstance = dagRun.task_instances.find((ti) => ti.task_id === node.id);

          return {
            ...node,
            data: {
              ...node.data,
              isSelected: node.id === taskId,
              taskInstance,
            },
          };
        });

  return (
    <Flex flex={1}>
      <ReactFlow
        colorMode={colorMode}
        defaultEdgeOptions={{ zIndex: 1 }}
        edges={data?.edges ?? []}
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
          zoomable
        />
      </ReactFlow>
    </Flex>
  );
};
