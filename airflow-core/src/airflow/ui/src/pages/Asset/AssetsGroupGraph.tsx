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
import { Box } from "@chakra-ui/react";
import { ReactFlow, Controls, Background, MiniMap } from "@xyflow/react";
import type { Node as ReactFlowNode } from "@xyflow/react";

import { DownloadButton } from "src/components/Graph/DownloadButton";
import { edgeTypes, nodeTypes } from "src/components/Graph/graphTypes";
import { useGraphLayout } from "src/components/Graph/useGraphLayout";
import { useColorMode } from "src/context/colorMode";
import { useMultiDependencyGraph } from "src/queries/useMultiDependencyGraph";

type AssetsGroupGraphProps = {
  readonly assetIds: Array<string>;
  readonly groupName: string;
};

type CustomNodeProps = { [key: string]: unknown; isSelected?: boolean };

export const AssetsGroupGraph = ({ assetIds, groupName }: AssetsGroupGraphProps) => {
  const { data } = useMultiDependencyGraph(assetIds);
  const rawEdges = data?.edges ?? [];
  const rawNodes = data?.nodes ?? [];

  const { data: graphData } = useGraphLayout({
    direction: "RIGHT",
    edges: rawEdges,
    nodes: rawNodes,
    openGroupIds: [],
  });

  const nodes = (graphData?.nodes ?? []).map((node) => {
    const assetId = typeof node.id === "string" ? node.id.replace(/^asset:/u, "") : node.id;
    const isSelected = assetIds.includes(assetId);

    return { ...node, data: { ...node.data, isSelected } };
  });

  const edges = (graphData?.edges ?? []).map((edge) => {
    const sourceId = typeof edge.source === "string" ? edge.source.replace(/^asset:/u, "") : edge.source;
    const targetId = typeof edge.target === "string" ? edge.target.replace(/^asset:/u, "") : edge.target;
    const isSelected = assetIds.includes(sourceId) || assetIds.includes(targetId);

    return {
      ...edge,
      data: {
        ...edge.data,
        rest: {
          ...edge.data?.rest,
          isSelected,
        },
      },
    };
  });

  const { colorMode = "light" } = useColorMode();
  const [selectedDarkColor, selectedLightColor] = useToken("colors", ["gray.200", "gray.800"]);
  const selectedColor = colorMode === "dark" ? selectedDarkColor : selectedLightColor;

  const boxBg = "chakra-subtle-bg";

  return (
    <Box bg={boxBg} borderRadius="md" h="70vh" minH="400px" overflow="hidden">
      <ReactFlow
        colorMode={colorMode}
        defaultEdgeOptions={{ zIndex: 1 }}
        edges={edges}
        edgeTypes={edgeTypes}
        fitView
        maxZoom={1.5}
        minZoom={0.15}
        nodes={nodes}
        nodesDraggable={false}
        nodeTypes={nodeTypes}
        onlyRenderVisibleElements
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
        <DownloadButton name={groupName || "assets-group"} />
      </ReactFlow>
    </Box>
  );
};
