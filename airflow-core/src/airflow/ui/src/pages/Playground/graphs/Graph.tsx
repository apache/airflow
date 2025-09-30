/* eslint-disable i18next/no-literal-string */

/* eslint-disable react/jsx-max-depth */

/* eslint-disable max-lines */

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
import { Box, Collapsible, Heading, HStack, Text, VStack } from "@chakra-ui/react";
import { ReactFlow, Controls, Background, MiniMap } from "@xyflow/react";
import "@xyflow/react/dist/style.css";
import { useMemo } from "react";

import { DownloadButton } from "src/components/Graph/DownloadButton";
import { edgeTypes, nodeTypes } from "src/components/Graph/graphTypes";
import { useColorMode } from "src/context/colorMode";
import { OpenGroupsContext } from "src/context/openGroups/Context";
import { getReactFlowThemeStyle } from "src/theme";

type GraphProps = {
  readonly isOpen: boolean;
  readonly onToggle: () => void;
};

export const Graph = ({ isOpen, onToggle }: GraphProps) => {
  const { colorMode = "light" } = useColorMode();

  // Mock OpenGroups context for playground
  const mockOpenGroupsContext = useMemo(
    () => ({
      allGroupIds: [],
      openGroupIds: [],
      setAllGroupIds: () => {
        // Mock function for playground
      },
      setOpenGroupIds: () => {
        // Mock function for playground
      },
      toggleGroupId: () => {
        // Mock function for playground
      },
    }),
    [],
  );

  // Mock data for the playground graph
  const mockNodes = useMemo(
    () => [
      {
        data: {
          depth: 0,
          height: 60,
          isSelected: false,
          label: "start_task",
          operator: "DummyOperator",
          taskInstance: { state: "success" },
          width: 120,
        },
        id: "start_task",
        position: { x: 50, y: 100 },
        type: "task",
      },
      {
        data: {
          depth: 0,
          height: 60,
          isSelected: false,
          label: "extract_data",
          operator: "PythonOperator",
          taskInstance: { state: "running" },
          width: 120,
        },
        id: "extract_data",
        position: { x: 220, y: 100 },
        type: "task",
      },
      {
        data: {
          depth: 0,
          height: 60,
          isSelected: false,
          label: "transform_data",
          operator: "PythonOperator",
          taskInstance: { state: "queued" },
          width: 120,
        },
        id: "transform_data",
        position: { x: 390, y: 100 },
        type: "task",
      },
      {
        data: {
          depth: 0,
          height: 60,
          isSelected: true,
          label: "load_data",
          operator: "PostgresOperator",
          taskInstance: { state: "failed" },
          width: 120,
        },
        id: "load_data",
        position: { x: 560, y: 100 },
        type: "task",
      },
      {
        data: {
          depth: 0,
          height: 60,
          isSelected: false,
          label: "end_task",
          operator: "DummyOperator",
          taskInstance: { state: "skipped" },
          width: 120,
        },
        id: "end_task",
        position: { x: 730, y: 100 },
        type: "task",
      },
    ],
    [],
  );

  const mockEdges = useMemo(
    () => [
      {
        data: {
          rest: {
            sections: [
              {
                endPoint: { x: 220, y: 130 },
                id: "start_task-extract_data-section",
                startPoint: { x: 170, y: 130 },
              },
            ],
          },
        },
        id: "start_task-extract_data",
        source: "start_task",
        target: "extract_data",
        type: "custom",
      },
      {
        data: {
          rest: {
            sections: [
              {
                endPoint: { x: 390, y: 130 },
                id: "extract_data-transform_data-section",
                startPoint: { x: 340, y: 130 },
              },
            ],
          },
        },
        id: "extract_data-transform_data",
        source: "extract_data",
        target: "transform_data",
        type: "custom",
      },
      {
        data: {
          rest: {
            sections: [
              {
                endPoint: { x: 560, y: 130 },
                id: "transform_data-load_data-section",
                startPoint: { x: 510, y: 130 },
              },
            ],
          },
        },
        id: "transform_data-load_data",
        source: "transform_data",
        target: "load_data",
        type: "custom",
      },
      {
        data: {
          rest: {
            sections: [
              {
                endPoint: { x: 730, y: 130 },
                id: "load_data-end_task-section",
                startPoint: { x: 680, y: 130 },
              },
            ],
          },
        },
        id: "load_data-end_task",
        source: "load_data",
        target: "end_task",
        type: "custom",
      },
    ],
    [],
  );

  return (
    <Box id="graphs">
      <Collapsible.Root onOpenChange={onToggle} open={isOpen}>
        <Collapsible.Trigger
          _hover={{ bg: "bg.subtle" }}
          borderColor={isOpen ? "brand.emphasized" : "border.muted"}
          borderWidth="1px"
          cursor="pointer"
          paddingX="6"
          paddingY="4"
          transition="all 0.2s"
          width="full"
        >
          <HStack justify="space-between" width="full">
            <VStack align="flex-start" gap="1">
              <Heading size="xl">Graph Components</Heading>
              <Text color="fg.muted" fontSize="sm">
                Flow diagrams and network visualizations
              </Text>
            </VStack>
            <Text color="brand.solid" fontSize="lg">
              {isOpen ? "−" : "+"}
            </Text>
          </HStack>
        </Collapsible.Trigger>
        <Collapsible.Content>
          <Box borderColor="border.muted" borderTop="none" borderWidth="1px" padding="6">
            <VStack align="stretch" gap={6}>
              <HStack align="flex-start" gap={6}>
                {/* ReactFlow Graph */}
                <VStack align="stretch" flex="2" gap={4} minWidth="400px">
                  <VStack align="stretch" gap={1}>
                    <Heading size="lg">Task Flow Graph</Heading>
                    <Text color="fg.muted" fontSize="sm">
                      Interactive DAG visualization using ReactFlow
                    </Text>
                  </VStack>
                  <Box height="400px" position="relative" width="100%">
                    <OpenGroupsContext.Provider value={mockOpenGroupsContext}>
                      <ReactFlow
                        colorMode={colorMode}
                        defaultEdgeOptions={{ zIndex: 1 }}
                        edges={mockEdges}
                        edgeTypes={edgeTypes}
                        fitView
                        maxZoom={1.5}
                        minZoom={0.25}
                        nodes={mockNodes}
                        nodesDraggable={false}
                        nodeTypes={nodeTypes}
                        onlyRenderVisibleElements
                        style={getReactFlowThemeStyle(colorMode)}
                      >
                        <Background />
                        <Controls showInteractive={false} />
                        <MiniMap nodeStrokeWidth={15} pannable style={{ height: 100, width: 150 }} zoomable />
                        <DownloadButton name="playground-dag" />
                      </ReactFlow>
                    </OpenGroupsContext.Provider>
                  </Box>
                </VStack>

                {/* Graph Legend */}
                <VStack align="stretch" flex="1" gap={4} minWidth="250px">
                  <VStack align="stretch" gap={1}>
                    <Heading size="lg">State Legend</Heading>
                    <Text color="fg.muted" fontSize="sm">
                      Task state indicators
                    </Text>
                  </VStack>
                  <VStack align="stretch" gap={3}>
                    <HStack>
                      <Box bg="success.solid" height="4" title="Success state" width="4" />
                      <Text fontSize="sm">Success - Task completed successfully</Text>
                    </HStack>
                    <HStack>
                      <Box bg="running.solid" height="4" title="Running state" width="4" />
                      <Text fontSize="sm">Running - Task is executing</Text>
                    </HStack>
                    <HStack>
                      <Box bg="failed.solid" height="4" title="Failed state" width="4" />
                      <Text fontSize="sm">Failed - Task execution failed</Text>
                    </HStack>
                    <HStack>
                      <Box bg="queued.solid" height="4" title="Queued state" width="4" />
                      <Text fontSize="sm">Queued - Task is waiting</Text>
                    </HStack>
                    <HStack>
                      <Box bg="skipped.solid" height="4" title="Skipped state" width="4" />
                      <Text fontSize="sm">Skipped - Task was skipped</Text>
                    </HStack>
                  </VStack>
                </VStack>

                {/* Graph Controls */}
                <VStack align="stretch" flex="1" gap={4} minWidth="300px">
                  <VStack align="stretch" gap={1}>
                    <Heading size="lg">Graph Controls</Heading>
                    <Text color="fg.muted" fontSize="sm">
                      Accessibility features
                    </Text>
                  </VStack>
                  <VStack align="stretch" gap={3}>
                    <Text fontSize="sm" fontWeight="semibold">
                      Keyboard Navigation:
                    </Text>
                    <VStack align="stretch" gap={2}>
                      <Text fontSize="sm">• Arrow keys: Pan graph</Text>
                      <Text fontSize="sm">• +/- keys: Zoom in/out</Text>
                      <Text fontSize="sm">• Tab: Navigate between nodes</Text>
                      <Text fontSize="sm">• Enter: Select/activate node</Text>
                      <Text fontSize="sm">• Escape: Clear selection</Text>
                    </VStack>

                    <Text fontSize="sm" fontWeight="semibold">
                      Screen Reader Support:
                    </Text>
                    <VStack align="stretch" gap={2}>
                      <Text fontSize="sm">• Node labels announced</Text>
                      <Text fontSize="sm">• State information included</Text>
                      <Text fontSize="sm">• Edge relationships described</Text>
                      <Text fontSize="sm">• ARIA landmarks for sections</Text>
                    </VStack>
                  </VStack>
                </VStack>
              </HStack>
            </VStack>
          </Box>
        </Collapsible.Content>
      </Collapsible.Root>
    </Box>
  );
};
