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
import { Flex } from "@chakra-ui/react";
import { ReactFlow, Controls, Background, MiniMap } from "@xyflow/react";
import "@xyflow/react/dist/style.css";
import { useParams } from "react-router-dom";

import { useColorMode } from "src/context/colorMode";
import useToggleGroups from "src/utils/useToggleGroups";

import Edge from "./Edge";
import { JoinNode } from "./JoinNode";
import { TaskNode } from "./TaskNode";
import { graphData } from "./data";
import { useGraphLayout } from "./useGraphLayout";

const nodeTypes = {
  join: JoinNode,
  task: TaskNode,
};
const edgeTypes = { custom: Edge };

export const Graph = () => {
  const { colorMode } = useColorMode();
  const { dagId = "" } = useParams();

  const { onToggleGroups, openGroupIds } = useToggleGroups({ dagId });
  const { data } = useGraphLayout({
    ...graphData,
    onToggleGroups,
    openGroupIds,
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
        nodes={data?.nodes ?? []}
        nodesDraggable={false}
        nodeTypes={nodeTypes}
        onlyRenderVisibleElements
        // fitViewOptions={{
        //   nodes: selected.taskId ? [{ id: selected.taskId }] : undefined,
        // }}
      >
        <Background />
        <Controls showInteractive={false} />
        <MiniMap
          // nodeColor={nodeColor}
          // nodeStrokeColor={(props) => nodeStrokeColor(props, colors)}
          nodeStrokeWidth={15}
          pannable
          zoomable
        />
      </ReactFlow>
    </Flex>
  );
};
