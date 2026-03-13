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
import { Box } from "@chakra-ui/react";
import type { VirtualItem } from "@tanstack/react-virtual";
import { useParams } from "react-router-dom";

import type { GridRunsResponse } from "openapi/requests";
import type { LightGridTaskInstanceSummary } from "openapi/requests/types.gen";
import { VersionIndicatorOptions } from "src/constants/showVersionIndicatorOptions";
import { useHover } from "src/context/hover";
import { useGridTiSummaries } from "src/queries/useGridTISummaries.ts";

import { GridTI } from "./GridTI";
import { DagVersionIndicator } from "./VersionIndicator";
import type { GridTask } from "./utils";

type Props = {
  readonly nodes: Array<GridTask>;
  readonly onCellClick?: () => void;
  readonly run: GridRunsResponse;
  readonly showVersionIndicatorMode?: VersionIndicatorOptions;
  readonly virtualItems?: Array<VirtualItem>;
};

const ROW_HEIGHT = 20;

export const TaskInstancesColumn = ({
  nodes,
  onCellClick,
  run,
  showVersionIndicatorMode,
  virtualItems,
}: Props) => {
  const { dagId = "", runId } = useParams();
  const isSelected = runId === run.run_id;
  const { data: gridTISummaries } = useGridTiSummaries({
    dagId,
    isSelected,
    runId: run.run_id,
    state: run.state,
  });
  const { hoveredRunId, setHoveredRunId } = useHover();

  const itemsToRender =
    virtualItems ?? nodes.map((_, index) => ({ index, size: ROW_HEIGHT, start: index * ROW_HEIGHT }));

  const taskInstances = gridTISummaries?.task_instances ?? [];

  const taskInstanceMap = new Map<string, LightGridTaskInstanceSummary>();

  for (const ti of taskInstances) {
    taskInstanceMap.set(ti.task_id, ti);
  }

  const versionNumbers = new Set(
    taskInstances.map((ti) => ti.dag_version_number).filter((vn) => vn !== null && vn !== undefined),
  );
  const hasMixedVersions = versionNumbers.size > 1;

  const isHovered = hoveredRunId === run.run_id;

  const handleMouseEnter = () => setHoveredRunId(run.run_id);
  const handleMouseLeave = () => setHoveredRunId(undefined);

  return (
    <Box
      bg={isSelected ? "brand.emphasized" : isHovered ? "brand.muted" : undefined}
      onMouseEnter={handleMouseEnter}
      onMouseLeave={handleMouseLeave}
      position="relative"
      transition="background-color 0.2s"
      width="18px"
    >
      {itemsToRender.map((virtualItem, idx) => {
        const node = nodes[virtualItem.index];

        if (!node) {
          return undefined;
        }

        const taskInstance = taskInstanceMap.get(node.id);

        if (!taskInstance) {
          return (
            <Box
              height={`${ROW_HEIGHT}px`}
              key={`${node.id}-${run.run_id}`}
              left={0}
              position="absolute"
              top={0}
              transform={`translateY(${virtualItem.start}px)`}
              width="18px"
            />
          );
        }

        let hasVersionChangeFlag = false;

        if (
          hasMixedVersions &&
          (showVersionIndicatorMode === VersionIndicatorOptions.DAG_VERSION ||
            showVersionIndicatorMode === VersionIndicatorOptions.ALL) &&
          idx > 0
        ) {
          const prevVirtualItem = itemsToRender[idx - 1];
          const prevNode = prevVirtualItem ? nodes[prevVirtualItem.index] : undefined;
          const prevTaskInstance = prevNode ? taskInstanceMap.get(prevNode.id) : undefined;

          hasVersionChangeFlag = Boolean(
            prevTaskInstance && prevTaskInstance.dag_version_number !== taskInstance.dag_version_number,
          );
        }

        return (
          <Box
            key={node.id}
            left={0}
            position="absolute"
            top={0}
            transform={`translateY(${virtualItem.start}px)`}
          >
            {hasVersionChangeFlag && (
              <DagVersionIndicator
                dagVersionNumber={taskInstance.dag_version_number ?? undefined}
                orientation="horizontal"
              />
            )}
            <GridTI
              dagId={dagId}
              instance={taskInstance}
              isGroup={node.isGroup}
              isMapped={node.is_mapped}
              label={node.label}
              onClick={onCellClick}
              runId={run.run_id}
              taskId={node.id}
            />
          </Box>
        );
      })}
    </Box>
  );
};
