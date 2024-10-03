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

import React, { useRef, useState, useEffect, useMemo } from "react";
import { Box, useTheme, Select, Text, Switch, Flex } from "@chakra-ui/react";
import ReactFlow, {
  ReactFlowProvider,
  Controls,
  Background,
  MiniMap,
  useReactFlow,
  Panel,
  useOnViewportChange,
  Viewport,
} from "reactflow";

import {
  useDagDetails,
  useDatasetEvents,
  useDatasets,
  useGraphData,
  useGridData,
  useUpstreamDatasetEvents,
} from "src/api";
import useSelection from "src/dag/useSelection";
import { getMetaValue, getTask, useOffsetTop } from "src/utils";
import { useGraphLayout } from "src/utils/graph";
import Edge from "src/components/Graph/Edge";
import type { DepNode, WebserverEdge } from "src/types";

import Node from "./Node";
import { buildEdges, nodeStrokeColor, nodeColor, flattenNodes } from "./utils";

const nodeTypes = { custom: Node };
const edgeTypes = { custom: Edge };

interface Props {
  openGroupIds: string[];
  onToggleGroups: (groupIds: string[]) => void;
  hoveredTaskState?: string | null;
}

const dagId = getMetaValue("dag_id");

type DatasetExpression = {
  all?: (string | DatasetExpression)[];
  any?: (string | DatasetExpression)[];
};

const getUpstreamDatasets = (
  datasetExpression: DatasetExpression,
  firstChildId: string,
  level = 0
) => {
  let edges: WebserverEdge[] = [];
  let nodes: DepNode[] = [];
  let type: DepNode["value"]["class"] | undefined;
  const datasetIds: string[] = [];
  let nestedExpression: DatasetExpression | undefined;
  if (datasetExpression?.any) {
    type = "or-gate";
    datasetExpression.any.forEach((de) => {
      if (typeof de === "string") datasetIds.push(de);
      else nestedExpression = de;
    });
  } else if (datasetExpression?.all) {
    type = "and-gate";
    datasetExpression.all.forEach((de) => {
      if (typeof de === "string") datasetIds.push(de);
      else nestedExpression = de;
    });
  }

  if (type && datasetIds.length) {
    edges.push({
      sourceId: `${type}-${level}`,
      // Point upstream datasets to the first task
      targetId: firstChildId,
      isSourceDataset: level === 0,
    });
    nodes.push({
      id: `${type}-${level}`,
      value: {
        class: type,
        label: "",
      },
    });
    datasetIds.forEach((d: string) => {
      nodes.push({
        id: d,
        value: {
          class: "asset",
          label: d,
        },
      });
      edges.push({
        sourceId: d,
        targetId: `${type}-${level}`,
      });
    });

    if (nestedExpression) {
      const data = getUpstreamDatasets(
        nestedExpression,
        `${type}-${level}`,
        (level += 1)
      );
      edges = [...edges, ...data.edges];
      nodes = [...nodes, ...data.nodes];
    }
  }
  return {
    nodes,
    edges,
  };
};

const Graph = ({ openGroupIds, onToggleGroups, hoveredTaskState }: Props) => {
  const graphRef = useRef(null);
  const { data } = useGraphData();
  const [arrange, setArrange] = useState(data?.arrange || "LR");
  const [hasRendered, setHasRendered] = useState(false);
  const [isZoomedOut, setIsZoomedOut] = useState(false);
  const { selected } = useSelection();
  const [showDatasets, setShowDatasets] = useState(true);

  const {
    data: { dagRuns, groups },
  } = useGridData();

  const { data: dagDetails } = useDagDetails();

  useEffect(() => {
    setArrange(data?.arrange || "LR");
  }, [data?.arrange]);

  const { data: datasetsCollection } = useDatasets({
    dagIds: [dagId],
  });

  let datasetNodes: DepNode[] = [];
  let datasetEdges: WebserverEdge[] = [];

  const { nodes: upstreamDatasetNodes, edges: upstreamDatasetEdges } =
    getUpstreamDatasets(
      dagDetails.datasetExpression as DatasetExpression,
      data?.nodes?.children?.[0]?.id ?? ""
    );

  const {
    data: { datasetEvents: upstreamDatasetEvents = [] },
  } = useUpstreamDatasetEvents({
    dagId,
    dagRunId: selected.runId || "",
    options: {
      enabled:
        !!upstreamDatasetNodes.length && !!selected.runId && showDatasets,
    },
  });

  const {
    data: { datasetEvents: downstreamDatasetEvents = [] },
  } = useDatasetEvents({
    sourceDagId: dagId,
    sourceRunId: selected.runId || undefined,
    options: { enabled: !!selected.runId && showDatasets },
  });

  if (showDatasets) {
    datasetNodes = [...upstreamDatasetNodes];
    datasetEdges = [...upstreamDatasetEdges];
    datasetsCollection?.datasets?.forEach((dataset) => {
      const producingTask = dataset?.producingTasks?.find(
        (t) => t.dagId === dagId
      );
      if (dataset.uri) {
        // check that the task is in the graph
        if (
          producingTask?.taskId &&
          getTask({ taskId: producingTask?.taskId, task: groups })
        ) {
          datasetEdges.push({
            sourceId: producingTask.taskId,
            targetId: dataset.uri,
          });
          datasetNodes.push({
            id: dataset.uri,
            value: {
              class: "asset",
              label: dataset.uri,
            },
          });
        }
      }
    });

    // Check if there is a dataset event even though we did not find a dataset
    downstreamDatasetEvents.forEach((de) => {
      const hasNode = datasetNodes.find((node) => node.id === de.datasetUri);
      if (!hasNode && de.sourceTaskId && de.datasetUri) {
        datasetEdges.push({
          sourceId: de.sourceTaskId,
          targetId: de.datasetUri,
        });
        datasetNodes.push({
          id: de.datasetUri,
          value: {
            class: "asset",
            label: de.datasetUri,
          },
        });
      }
    });
  }

  const { data: graphData } = useGraphLayout({
    edges: [...(data?.edges || []), ...datasetEdges],
    nodes: data?.nodes
      ? {
          ...data.nodes,
          children: [...(data?.nodes.children || []), ...datasetNodes],
        }
      : data?.nodes,
    openGroupIds,
    arrange,
  });

  const { colors } = useTheme();
  const { getZoom, fitView } = useReactFlow();
  const latestDagRunId = dagRuns[dagRuns.length - 1]?.runId;
  const offsetTop = useOffsetTop(graphRef);

  useOnViewportChange({
    onEnd: (viewport: Viewport) => {
      if (viewport.zoom < 0.5 && !isZoomedOut) setIsZoomedOut(true);
      if (viewport.zoom >= 0.5 && isZoomedOut) setIsZoomedOut(false);
    },
  });

  const { nodes, edges: nodeEdges } = useMemo(
    () =>
      flattenNodes({
        children: graphData?.children,
        selected,
        openGroupIds,
        onToggleGroups,
        latestDagRunId,
        groups,
        hoveredTaskState,
        isZoomedOut,
        datasetEvents: selected.runId
          ? [...upstreamDatasetEvents, ...downstreamDatasetEvents]
          : [],
      }),
    [
      graphData?.children,
      selected,
      openGroupIds,
      onToggleGroups,
      latestDagRunId,
      groups,
      hoveredTaskState,
      isZoomedOut,
      upstreamDatasetEvents,
      downstreamDatasetEvents,
    ]
  );

  // Zoom to/from nodes when changing selection, maintain zoom level when changing task selection
  useEffect(() => {
    if (hasRendered) {
      const zoom = getZoom();
      if (zoom < 0.5) setIsZoomedOut(true);
      fitView({
        duration: 750,
        nodes: selected.taskId ? [{ id: selected.taskId }] : undefined,
        minZoom: selected.taskId ? zoom : undefined,
        maxZoom: selected.taskId ? zoom : undefined,
      });
    }
    setHasRendered(true);
  }, [fitView, hasRendered, selected.taskId, getZoom]);

  // merge & dedupe edges
  const flatEdges = [...(graphData?.edges || []), ...(nodeEdges || [])].filter(
    (value, index, self) => index === self.findIndex((t) => t.id === value.id)
  );

  const edges = buildEdges({
    edges: flatEdges,
    nodes,
    selectedTaskId: selected.taskId,
    isZoomedOut,
  });

  return (
    <Box
      ref={graphRef}
      height={`calc(100% - ${offsetTop}px)`}
      borderWidth={1}
      borderColor="gray.200"
    >
      {!!offsetTop && (
        <ReactFlow
          nodes={nodes}
          edges={edges}
          nodesDraggable={false}
          nodeTypes={nodeTypes}
          edgeTypes={edgeTypes}
          minZoom={0.25}
          maxZoom={1}
          onlyRenderVisibleElements
          defaultEdgeOptions={{ zIndex: 1 }}
          // Fit view to selected task or the whole graph on render
          fitView
          fitViewOptions={{
            nodes: selected.taskId ? [{ id: selected.taskId }] : undefined,
          }}
        >
          <Panel position="top-right">
            <Box bg={colors.whiteAlpha[800]} p={1}>
              {!!datasetsCollection?.datasets?.length && (
                <Flex display="flex" alignItems="center">
                  <Text fontSize="sm" mr={1}>
                    Show datasets:
                  </Text>
                  <Switch
                    id="show-datasets"
                    isChecked={showDatasets}
                    onChange={() => setShowDatasets(!showDatasets)}
                  />
                </Flex>
              )}
              <Text fontSize="sm">Layout:</Text>
              <Select
                value={arrange}
                onChange={(e) => setArrange(e.target.value)}
                fontSize="sm"
                size="sm"
              >
                <option value="LR">Left -&gt; Right</option>
                <option value="RL">Right -&gt; Left</option>
                <option value="TB">Top -&gt; Bottom</option>
                <option value="BT">Bottom -&gt; Top</option>
              </Select>
            </Box>
          </Panel>
          <Background />
          <Controls showInteractive={false} />
          <MiniMap
            nodeStrokeWidth={15}
            nodeStrokeColor={(props) => nodeStrokeColor(props, colors)}
            nodeColor={nodeColor}
            zoomable
            pannable
          />
        </ReactFlow>
      )}
    </Box>
  );
};

const GraphWrapper = (props: Props) => (
  <ReactFlowProvider>
    <Graph {...props} />
  </ReactFlowProvider>
);

export default GraphWrapper;
