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
import { Box, Text, useToken } from "@chakra-ui/react";
import {
  ReactFlow,
  Controls,
  Background,
  MiniMap,
  useReactFlow,
  type NodeMouseHandler,
  type Node as ReactFlowNode,
} from "@xyflow/react";
import "@xyflow/react/dist/style.css";
import { useEffect, useMemo, useState, type Dispatch, type SetStateAction } from "react";
import { useTranslation } from "react-i18next";
import { useParams } from "react-router-dom";

import type { AssetResponse, EdgeResponse, NodeResponse } from "openapi/requests/types.gen";
import { ErrorAlert } from "src/components/ErrorAlert";
import { DownloadButton } from "src/components/Graph/DownloadButton";
import { edgeTypes, nodeTypes } from "src/components/Graph/graphTypes";
import type { CustomNodeProps } from "src/components/Graph/reactflowUtils";
import { useGraphLayout } from "src/components/Graph/useGraphLayout";
import { ProgressBar } from "src/components/ui";
import { useColorMode } from "src/context/colorMode";
import { useAssetLineage } from "src/queries/useAssetLineage";
import { getReactFlowThemeStyle } from "src/theme";

import { getHighlightedLineage, type LineageDirection } from "./lineageHighlightUtils";

export const AssetLineageGraph = ({
  activeNodeId,
  asset,
  highlightDirection,
  searchTerm,
  setActiveNodeId,
}: {
  readonly activeNodeId?: string;
  readonly asset?: AssetResponse;
  readonly highlightDirection: LineageDirection;
  readonly searchTerm: string;
  readonly setActiveNodeId: Dispatch<SetStateAction<string | undefined>>;
}) => {
  const { assetId } = useParams();
  const { colorMode = "light" } = useColorMode();
  const { setCenter } = useReactFlow();
  const { t: translate } = useTranslation(["assets"]);
  const [hoveredNodeId, setHoveredNodeId] = useState<string | undefined>(undefined);

  // Fetch the lineage graph data
  const {
    data: lineageData = { edges: [], nodes: [] },
    error,
    isError,
    isLoading,
  } = useAssetLineage(assetId);

  const mappedNodes: Array<NodeResponse> = useMemo(
    () =>
      lineageData.nodes.map((node) => ({
        data: {
          id: node.id,
          label: node.name,
          rest: node,
        },
        id: node.id,
        label: node.name,
        position: { x: 0, y: 0 },
        type: node.node_type === "task" ? "task" : node.node_type === "dag" ? "dag" : "asset",
      })),
    [lineageData.nodes],
  );

  const mappedEdges: Array<EdgeResponse> = useMemo(
    () =>
      lineageData.edges.map((edge) => ({
        id: `${edge.source_id}-${edge.target_id}`,
        source: edge.source_id,
        source_id: edge.source_id,
        target: edge.target_id,
        target_id: edge.target_id,
        type: "custom",
      })),
    [lineageData.edges],
  );

  const highlightedNodeId = hoveredNodeId ?? activeNodeId;

  const { highlightedEdgeIds, highlightedNodeIds } = useMemo(
    () =>
      getHighlightedLineage({
        direction: highlightDirection,
        edges: lineageData.edges,
        nodeId: highlightedNodeId,
      }),
    [highlightDirection, highlightedNodeId, lineageData.edges],
  );

  // Automatically layout the converted nodes
  const { data: layoutData } = useGraphLayout({
    direction: "RIGHT",
    edges: mappedEdges,
    nodes: mappedNodes,
    openGroupIds: [],
  });

  const [selectedDarkColor, selectedLightColor] = useToken("colors", ["bg.muted", "bg.emphasized"]);
  const selectedColor = colorMode === "dark" ? selectedDarkColor : selectedLightColor;
  const layoutEdges = useMemo(
    () =>
      (layoutData?.edges ?? []).map((edge) => ({
        ...edge,
        data: {
          rest: {
            ...edge.data?.rest,
            isSelected: highlightedEdgeIds.has(edge.id),
            lineageDirection: highlightedEdgeIds.has(edge.id) ? highlightDirection : undefined,
          },
        },
      })),
    [highlightDirection, highlightedEdgeIds, layoutData?.edges],
  );
  const layoutNodes = useMemo(
    () =>
      (layoutData?.nodes ?? []).map((node) => {
        const lineageStyle =
          node.id === highlightedNodeId
            ? "focus"
            : highlightedNodeIds.has(node.id)
              ? highlightDirection
              : undefined;

        return {
          ...node,
          data: {
            ...node.data,
            isSelected: lineageStyle !== undefined,
            lineageStyle,
          },
        };
      }),
    [highlightDirection, highlightedNodeId, highlightedNodeIds, layoutData?.nodes],
  );

  useEffect(() => {
    const trimmedSearch = searchTerm.trim().toLowerCase();

    if (trimmedSearch === "" || layoutNodes.length === 0) {
      return;
    }

    const matchedNode = layoutNodes.find((node) => {
      const nodeId = node.id.toLowerCase();
      const nodeLabel = node.data.label.toLowerCase();

      return nodeLabel.includes(trimmedSearch) || nodeId.includes(trimmedSearch);
    });

    if (!matchedNode) {
      return;
    }

    setActiveNodeId(matchedNode.id);
    void setCenter(
      matchedNode.position.x + (matchedNode.width ?? 0) / 2,
      matchedNode.position.y + (matchedNode.height ?? 0) / 2,
      { duration: 300, zoom: 1 },
    );
  }, [layoutNodes, searchTerm, setActiveNodeId, setCenter]);

  if (isLoading) {
    return <ProgressBar size="xs" visibility="visible" />;
  }

  if (isError) {
    return <ErrorAlert error={error} />;
  }

  if (layoutNodes.length === 0) {
    return (
      <Box p={4}>
        <Text color="fg.muted">{translate("no_lineage_data_found")}</Text>
      </Box>
    );
  }

  const onNodeMouseEnter: NodeMouseHandler = (_event, node) => {
    setHoveredNodeId(node.id);
  };
  const onNodeMouseLeave: NodeMouseHandler = () => {
    setHoveredNodeId(undefined);
  };

  return (
    <ReactFlow
      colorMode={colorMode}
      defaultEdgeOptions={{ zIndex: 1 }}
      edges={layoutEdges}
      edgeTypes={edgeTypes}
      fitView
      maxZoom={1.5}
      minZoom={0.25}
      nodes={layoutNodes}
      nodesDraggable={false}
      nodeTypes={nodeTypes}
      onlyRenderVisibleElements
      onNodeClick={(_, node) => {
        setActiveNodeId(node.id);
      }}
      onNodeMouseEnter={onNodeMouseEnter}
      onNodeMouseLeave={onNodeMouseLeave}
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
      <DownloadButton name={`lineage-${asset?.name ?? assetId}`} />
    </ReactFlow>
  );
};
