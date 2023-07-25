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

import React, { useEffect } from "react";
import ReactFlow, {
  ReactFlowProvider,
  Controls,
  Background,
  MiniMap,
  Node as ReactFlowNode,
  useReactFlow,
  ControlButton,
  Panel,
} from "reactflow";
import { Box, Tooltip, useTheme } from "@chakra-ui/react";
import { RiFocus3Line } from "react-icons/ri";

import { useDatasetDependencies } from "src/api";
import Edge from "src/components/Graph/Edge";
import { useContainerRef } from "src/context/containerRef";

import Node, { CustomNodeProps } from "./Node";
import Legend from "./Legend";

interface Props {
  onSelect: (datasetId: string) => void;
  selectedUri: string | null;
}

const nodeTypes = { custom: Node };
const edgeTypes = { custom: Edge };

const Graph = ({ onSelect, selectedUri }: Props) => {
  const { data } = useDatasetDependencies();
  const { colors } = useTheme();
  const { setCenter, setViewport } = useReactFlow();
  const containerRef = useContainerRef();

  useEffect(() => {
    setViewport({ x: 0, y: 0, zoom: 1 });
  }, [selectedUri, setViewport]);

  const nodeColor = ({
    data: { isSelected },
  }: ReactFlowNode<CustomNodeProps>) =>
    isSelected ? colors.blue["300"] : colors.gray["300"];

  if (!data || !data.fullGraph || !data.subGraphs) return null;
  const graph = selectedUri
    ? data.subGraphs.find((g) =>
        g.children.some((n) => n.id === `dataset:${selectedUri}`)
      )
    : data.fullGraph;
  if (!graph) return null;

  const edges = graph.edges.map((e) => ({
    id: e.id,
    source: e.sources[0],
    target: e.targets[0],
    type: "custom",
    data: {
      rest: {
        ...e,
        isSelected: selectedUri && e.id.includes(selectedUri),
      },
    },
  }));

  const nodes: ReactFlowNode<CustomNodeProps>[] = graph.children.map((c) => ({
    id: c.id,
    data: {
      label: c.value.label,
      type: c.value.class,
      width: c.width,
      height: c.height,
      onSelect,
      isSelected: selectedUri === c.value.label,
      isHighlighted: edges.some(
        (e) => e.data.rest.isSelected && e.id.includes(c.id)
      ),
    },
    type: "custom",
    position: {
      x: c.x || 0,
      y: c.y || 0,
    },
  }));

  const focusNode = () => {
    if (selectedUri) {
      const node = nodes.find((n) => n.data.label === selectedUri);
      if (!node || !node.position) return;
      const { x, y } = node.position;
      setCenter(
        x + (node.data.width || 0) / 2,
        y + (node.data.height || 0) / 2,
        {
          duration: 1000,
        }
      );
    }
  };

  return (
    <ReactFlow
      nodes={nodes}
      edges={edges}
      nodeTypes={nodeTypes}
      edgeTypes={edgeTypes}
      nodesDraggable={false}
      minZoom={0.25}
      maxZoom={1}
      onlyRenderVisibleElements
      defaultEdgeOptions={{ zIndex: 1 }}
    >
      <Background />
      <Controls showInteractive={false}>
        <ControlButton onClick={focusNode} disabled={!selectedUri}>
          <Tooltip
            portalProps={{ containerRef }}
            label="Center selected dataset"
            placement="right"
          >
            <Box>
              <RiFocus3Line
                size={16}
                style={{
                  // override react-flow css
                  maxWidth: "16px",
                  maxHeight: "16px",
                  color: colors.gray[800],
                }}
                aria-label="Center selected dataset"
              />
            </Box>
          </Tooltip>
        </ControlButton>
      </Controls>
      <Panel position="top-right">
        <Legend />
      </Panel>
      <MiniMap nodeStrokeWidth={15} nodeColor={nodeColor} zoomable pannable />
    </ReactFlow>
  );
};

const GraphWrapper = (props: Props) => (
  <ReactFlowProvider>
    <Graph {...props} />
  </ReactFlowProvider>
);

export default GraphWrapper;
