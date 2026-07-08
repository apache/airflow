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
import { memo } from "react";

import { Box, type BoxProps } from "@chakra-ui/react";
import type { VirtualItem } from "@tanstack/react-virtual";
import { useParams } from "react-router-dom";

import type { GridRunsResponse, GridTISummaries } from "openapi/requests";
import type { LightGridTaskInstanceSummary } from "openapi/requests/types.gen";
import { VersionIndicatorOptions } from "src/constants/showVersionIndicatorOptions";
import { useHover } from "src/context/hover";

import { GridTI } from "./GridTI";
import { DagVersionIndicator } from "./VersionIndicator";
import { ROW_HEIGHT } from "./constants";
import type { GridTask } from "./utils";

type Props = {
  readonly nodes: Array<GridTask>;
  readonly onCellClick?: () => void;
  readonly run: GridRunsResponse;
  readonly showVersionIndicatorMode?: VersionIndicatorOptions;
  readonly tiSummaries?: GridTISummaries;
  readonly virtualItems?: Array<VirtualItem>;
};

type CellBorderProps = Pick<BoxProps, "borderBottomWidth" | "borderColor" | "borderTopWidth">;

const taskInstanceCellBorderProps = (hideRowBorders: boolean, rowIndex: number): CellBorderProps =>
  hideRowBorders
    ? { borderBottomWidth: 0, borderTopWidth: 0 }
    : {
        borderBottomWidth: 1,
        borderColor: "border",
        borderTopWidth: rowIndex === 0 ? 1 : 0,
      };

const TaskInstancesColumnInner = memo(
  ({
    dagId,
    hideRowBorders,
    hoveredTaskId,
    isSelected,
    itemsToRender,
    nodes,
    onCellClick,
    run,
    setHoveredRunId,
    setHoveredTaskId,
    showVersionIndicatorMode,
    tiSummaries,
  }: Omit<Props, "virtualItems"> & {
    readonly dagId: string;
    readonly hideRowBorders: boolean;
    readonly hoveredTaskId?: string;
    readonly isSelected: boolean;
    readonly itemsToRender: Array<VirtualItem>;
    readonly setHoveredRunId: (runId: string | undefined) => void;
    readonly setHoveredTaskId: (taskId: string | undefined) => void;
  }) => {
    const taskInstances = tiSummaries?.task_instances ?? [];
    const taskInstanceMap = new Map<string, LightGridTaskInstanceSummary>();

    for (const ti of taskInstances) {
      taskInstanceMap.set(ti.task_id, ti);
    }

    const versionNumbers = new Set(
      taskInstances.map((ti) => ti.dag_version_number).filter((vn) => vn !== null && vn !== undefined),
    );
    const hasMixedVersions = versionNumbers.size > 1;

    const handleMouseEnter = () => setHoveredRunId(run.run_id);
    const handleMouseLeave = () => setHoveredRunId(undefined);

    return (
      <Box
        bg={isSelected ? "brand.emphasized" : hideRowBorders ? "brand.muted" : undefined}
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
                {...taskInstanceCellBorderProps(hideRowBorders, virtualItem.index)}
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
              {...taskInstanceCellBorderProps(hideRowBorders, virtualItem.index)}
              height={`${ROW_HEIGHT}px`}
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
                hasNote={taskInstance.has_note ?? false}
                instance={taskInstance}
                isGroup={node.isGroup}
                isHovered={hoveredTaskId === node.id}
                isMapped={node.is_mapped}
                label={node.label}
                onClick={onCellClick}
                runId={run.run_id}
                setHoveredTaskId={setHoveredTaskId}
                taskId={node.id}
              />
            </Box>
          );
        })}
      </Box>
    );
  },
  (prevProps, nextProps) => {
    if (
      prevProps.dagId !== nextProps.dagId ||
      prevProps.hideRowBorders !== nextProps.hideRowBorders ||
      prevProps.hoveredTaskId !== nextProps.hoveredTaskId ||
      prevProps.isSelected !== nextProps.isSelected ||
      prevProps.run.run_id !== nextProps.run.run_id ||
      prevProps.run.state !== nextProps.run.state ||
      prevProps.run.duration !== nextProps.run.duration ||
      prevProps.showVersionIndicatorMode !== nextProps.showVersionIndicatorMode ||
      prevProps.tiSummaries !== nextProps.tiSummaries ||
      prevProps.nodes.length !== nextProps.nodes.length
    ) {
      return false;
    }

    if (prevProps.itemsToRender.length !== nextProps.itemsToRender.length) {
      return false;
    }

    for (let i = 0; i < prevProps.itemsToRender.length; i++) {
      if (
        prevProps.itemsToRender[i].index !== nextProps.itemsToRender[i].index ||
        prevProps.itemsToRender[i].start !== nextProps.itemsToRender[i].start
      ) {
        return false;
      }
    }

    return true;
  },
);

export const TaskInstancesColumn = ({
  nodes,
  onCellClick,
  run,
  showVersionIndicatorMode,
  tiSummaries,
  virtualItems,
}: Props) => {
  const { dagId = "", runId } = useParams();
  const isSelected = runId === run.run_id;

  const { hoveredRunId, hoveredTaskId, setHoveredRunId, setHoveredTaskId } = useHover();

  const itemsToRender =
    virtualItems ?? nodes.map((_, index) => ({ index, size: ROW_HEIGHT, start: index * ROW_HEIGHT }));

  const isHovered = hoveredRunId === run.run_id;
  const hideRowBorders = isSelected || isHovered;

  return (
    <TaskInstancesColumnInner
      dagId={dagId}
      hideRowBorders={hideRowBorders}
      hoveredTaskId={hoveredTaskId}
      isSelected={isSelected}
      itemsToRender={itemsToRender}
      nodes={nodes}
      onCellClick={onCellClick}
      run={run}
      setHoveredRunId={setHoveredRunId}
      setHoveredTaskId={setHoveredTaskId}
      showVersionIndicatorMode={showVersionIndicatorMode}
      tiSummaries={tiSummaries}
    />
  );
};
