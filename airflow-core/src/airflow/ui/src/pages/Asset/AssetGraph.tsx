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

import type { AssetResponse } from "openapi/requests/types.gen";
import { DownloadButton } from "src/components/Graph/DownloadButton";
import { edgeTypes, nodeTypes } from "src/components/Graph/graphTypes";
import type { CustomNodeProps } from "src/components/Graph/reactflowUtils";
import { useGraphLayout } from "src/components/Graph/useGraphLayout";
import { useColorMode } from "src/context/colorMode";
import { useDependencyGraph } from "src/queries/useDependencyGraph";
import { getReactFlowThemeStyle } from "src/theme";
import { getTaskLevelDependencies } from "src/utils/assetGraph";

export const AssetGraph = ({
  asset,
  showDagLevelDependencies = true,
  showTaskLevelDependencies = false,
}: {
  readonly asset?: AssetResponse;
  readonly showDagLevelDependencies?: boolean;
  readonly showTaskLevelDependencies?: boolean;
}) => {
  const { assetId } = useParams();
  const { colorMode = "light" } = useColorMode();

  const { data: dagLevelGraphData = { edges: [], nodes: [] } } = useDependencyGraph(`asset:${assetId}`);
  const taskLevelGraphData = getTaskLevelDependencies({ edges: [], nodes: [] }, asset);

  const combinedGraphData = {
    edges: [
      ...(showDagLevelDependencies ? dagLevelGraphData.edges : []),
      ...(showTaskLevelDependencies ? taskLevelGraphData.edges : []),
    ],
    nodes: [
      // Always show the asset nodes from DAG level data
      ...dagLevelGraphData.nodes.filter((node) => node.id === `asset:${assetId}`),
      // Conditionally show dag nodes
      ...(showDagLevelDependencies
        ? dagLevelGraphData.nodes.filter((node) => node.id !== `asset:${assetId}`)
        : []),
      // Conditionally show task nodes
      ...(showTaskLevelDependencies ? taskLevelGraphData.nodes : []),
    ],
  };

  const { data: graphData } = useGraphLayout({
    ...combinedGraphData,
    direction: "RIGHT",
    openGroupIds: [],
  });

  const nodes = graphData?.nodes.map((node) =>
    node.id === `asset:${assetId}` ? { ...node, data: { ...node.data, isSelected: true } } : node,
  );

  const [selectedDarkColor, selectedLightColor] = useToken("colors", ["bg.muted", "bg.emphasized"]);

  const selectedColor = colorMode === "dark" ? selectedDarkColor : selectedLightColor;

  const edges = (graphData?.edges ?? []).map((edge) => ({
    ...edge,
    data: {
      ...edge.data,
      rest: {
        ...edge.data?.rest,
        edgeType:
          showTaskLevelDependencies && (edge.source.startsWith("task:") || edge.target.startsWith("task:"))
            ? "task"
            : "dag",
        isSelected: `asset:${asset?.id}` === edge.source || `asset:${asset?.id}` === edge.target,
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
        nodeStrokeColor={(node: ReactFlowNode<CustomNodeProps>) =>
          node.data.isSelected && selectedColor !== undefined ? selectedColor : ""
        }
        nodeStrokeWidth={15}
        pannable
        zoomable
      />
      <DownloadButton name={asset?.name ?? asset?.uri ?? "asset"} />
    </ReactFlow>
  );
};
