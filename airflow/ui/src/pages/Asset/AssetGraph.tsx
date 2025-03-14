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

import { useDependenciesServiceGetDependencies } from "openapi/queries";
import type { AssetResponse } from "openapi/requests/types.gen";
import { AliasNode } from "src/components/Graph/AliasNode";
import { AssetNode } from "src/components/Graph/AssetNode";
import { DagNode } from "src/components/Graph/DagNode";
import Edge from "src/components/Graph/Edge";
import type { CustomNodeProps } from "src/components/Graph/reactflowUtils";
import { useGraphLayout } from "src/components/Graph/useGraphLayout";
import { useColorMode } from "src/context/colorMode";

const nodeTypes = {
  asset: AssetNode,
  "asset-alias": AliasNode,
  dag: DagNode,
};
const edgeTypes = { custom: Edge };

export const AssetGraph = ({ asset }: { readonly asset?: AssetResponse }) => {
  const { colorMode = "light" } = useColorMode();

  const { data = { edges: [], nodes: [] } } = useDependenciesServiceGetDependencies(
    { nodeId: `asset:${asset?.name}` },
    undefined,
    { enabled: Boolean(asset) && Boolean(asset?.name) },
  );

  const { data: graphData } = useGraphLayout({
    ...data,
    dagId: asset?.name ?? "",
    direction: "RIGHT",
    openGroupIds: [],
  });

  const nodes = graphData?.nodes.map((node) =>
    node.data.label === asset?.name ? { ...node, data: { ...node.data, isSelected: true } } : node,
  );

  const [selectedDarkColor, selectedLightColor] = useToken("colors", ["gray.200", "gray.800"]);

  const selectedColor = colorMode === "dark" ? selectedDarkColor : selectedLightColor;

  const edges = (graphData?.edges ?? []).map((edge) => ({
    ...edge,
    data: {
      ...edge.data,
      rest: {
        ...edge.data?.rest,
        isSelected: `asset:${asset?.name}` === edge.source || `asset:${asset?.name}` === edge.target,
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
        nodeStrokeColor={(node: ReactFlowNode<CustomNodeProps>) =>
          node.data.isSelected && selectedColor !== undefined ? selectedColor : ""
        }
        nodeStrokeWidth={15}
        pannable
        zoomable
      />
    </ReactFlow>
  );
};
