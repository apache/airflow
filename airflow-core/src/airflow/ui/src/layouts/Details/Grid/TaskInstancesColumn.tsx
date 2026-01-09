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
import { memo, useCallback, useMemo } from "react";
import { useParams } from "react-router-dom";

import type { GridRunsResponse } from "openapi/requests";
import type { LightGridTaskInstanceSummary } from "openapi/requests/types.gen";
import { useHover } from "src/context/hover";
import { useGridTiSummaries } from "src/queries/useGridTISummaries.ts";

import { GridTI } from "./GridTI";
import type { GridTask } from "./utils";

type Props = {
  readonly nodes: Array<GridTask>;
  readonly onCellClick?: () => void;
  readonly run: GridRunsResponse;
  readonly virtualItems?: Array<VirtualItem>;
};

const ROW_HEIGHT = 20;

const TaskInstancesColumnInner = ({ nodes, onCellClick, run, virtualItems }: Props) => {
  const { dagId = "", runId } = useParams();
  const { data: gridTISummaries } = useGridTiSummaries({ dagId, runId: run.run_id, state: run.state });
  const { hoveredRunId, setHoveredRunId } = useHover();

  const itemsToRender =
    virtualItems ?? nodes.map((_, index) => ({ index, size: ROW_HEIGHT, start: index * ROW_HEIGHT }));

  const taskInstanceMap = useMemo(() => {
    const taskInstances = gridTISummaries?.task_instances ?? [];
    const map = new Map<string, LightGridTaskInstanceSummary>();

    for (const ti of taskInstances) {
      map.set(ti.task_id, ti);
    }

    return map;
  }, [gridTISummaries?.task_instances]);

  const isSelected = runId === run.run_id;
  const isHovered = hoveredRunId === run.run_id;

  const handleMouseEnter = useCallback(() => setHoveredRunId(run.run_id), [setHoveredRunId, run.run_id]);
  const handleMouseLeave = useCallback(() => setHoveredRunId(undefined), [setHoveredRunId]);

  return (
    <Box
      bg={isSelected ? "brand.emphasized" : isHovered ? "brand.muted" : undefined}
      onMouseEnter={handleMouseEnter}
      onMouseLeave={handleMouseLeave}
      position="relative"
      transition="background-color 0.2s"
      width="18px"
    >
      {itemsToRender.map((virtualItem) => {
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

        return (
          <Box
            key={node.id}
            left={0}
            position="absolute"
            top={0}
            transform={`translateY(${virtualItem.start}px)`}
          >
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

export const TaskInstancesColumn = memo(TaskInstancesColumnInner);
