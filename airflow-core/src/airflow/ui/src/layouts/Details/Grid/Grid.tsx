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
import { Box, Flex } from "@chakra-ui/react";
import { useVirtualizer } from "@tanstack/react-virtual";
import dayjs from "dayjs";
import dayjsDuration from "dayjs/plugin/duration";
import { useRef } from "react";
import type { RefObject } from "react";
import { useParams, useSearchParams } from "react-router-dom";

import type { DagRunState, DagRunType, GridRunsResponse } from "openapi/requests";
import type { VersionIndicatorOptions } from "src/constants/showVersionIndicatorOptions";
import { useOpenGroups } from "src/context/openGroups";
import { NavigationModes, useNavigation } from "src/hooks/navigation";
import { useGridRuns } from "src/queries/useGridRuns.ts";
import { useGridStructure } from "src/queries/useGridStructure.ts";
import { useGridTiSummariesStream } from "src/queries/useGridTISummaries.ts";
import { isStatePending } from "src/utils";

import { Bar } from "./Bar";
import { DurationAxis } from "./DurationAxis";
import { DurationTick } from "./DurationTick";
import { GridPaginationButtons } from "./GridPaginationButtons";
import { TaskInstancesColumn } from "./TaskInstancesColumn";
import { TaskNames } from "./TaskNames";
import { GANTT_ROW_OFFSET_PX, GRID_HEADER_HEIGHT_PX, GRID_HEADER_PADDING_PX, ROW_HEIGHT } from "./constants";
import { useGridPagination } from "./useGridPagination";
import { useGridRunsWithVersionFlags } from "./useGridRunsWithVersionFlags";
import { estimateTaskNameColumnWidthPx, flattenNodes } from "./utils";

dayjs.extend(dayjsDuration);

type Props = {
  readonly dagRunState?: DagRunState | undefined;
  readonly limit: number;
  readonly offset: number;
  readonly onJumpToLatest: () => void;
  readonly runAfterGte?: string;
  readonly runAfterLte?: string;
  readonly runType?: DagRunType | undefined;
  readonly setOffset: (value: number) => void;
  readonly sharedScrollContainerRef?: RefObject<HTMLDivElement | null>;
  readonly showGantt?: boolean;
  readonly showVersionIndicatorMode?: VersionIndicatorOptions;
  readonly triggeringUser?: string | undefined;
};

const GRID_INNER_SCROLL_PADDING_START_PX = GRID_HEADER_PADDING_PX + GRID_HEADER_HEIGHT_PX;

export const Grid = ({
  dagRunState,
  limit,
  offset,
  onJumpToLatest,
  runAfterGte,
  runAfterLte,
  runType,
  setOffset,
  sharedScrollContainerRef,
  showGantt,
  showVersionIndicatorMode,
  triggeringUser,
}: Props) => {
  const gridRef = useRef<HTMLDivElement>(null);
  const scrollContainerRef = useRef<HTMLDivElement | null>(null);

  const usesSharedScroll = Boolean(sharedScrollContainerRef && showGantt);

  const { openGroupIds, toggleGroupId } = useOpenGroups();
  const { dagId = "" } = useParams();
  const [searchParams] = useSearchParams();

  const filterRoot = searchParams.get("root") ?? undefined;
  const includeUpstream = searchParams.get("upstream") === "true";
  const includeDownstream = searchParams.get("downstream") === "true";
  const depthParam = searchParams.get("depth");
  const depth = depthParam !== null && depthParam !== "" ? parseInt(depthParam, 10) : undefined;

  // Over fetch gridRuns and then truncate to limit, in order to check pagination
  const { data: dataGridRuns, isLoading } = useGridRuns({
    dagRunState,
    limit: limit + 1,
    offset,
    runAfterGte,
    runAfterLte,
    runType,
    triggeringUser,
  });
  const gridRuns = dataGridRuns?.slice(0, limit);

  const { handleNewerRuns, handleOlderRuns, hasNewerRuns, hasOlderRuns, latestNotVisible } =
    useGridPagination({ gridRuns: dataGridRuns, limit, offset, setOffset });

  const { summariesByRunId } = useGridTiSummariesStream({
    dagId,
    runIds: gridRuns?.map((dr: GridRunsResponse) => dr.run_id) ?? [],
    states: gridRuns?.map((dr: GridRunsResponse) => dr.state),
  });

  const { data: dagStructure } = useGridStructure({
    dagRunState,
    depth,
    hasActiveRun: gridRuns?.some((dr) => isStatePending(dr.state)),
    includeDownstream,
    includeUpstream,
    limit,
    root: filterRoot,
    runType,
    triggeringUser,
  });

  // calculate dag run bar heights relative to max
  const max = Math.max.apply(
    undefined,
    gridRuns === undefined
      ? []
      : gridRuns
          .map((dr: GridRunsResponse) => dr.duration)
          .filter((duration: number | null): duration is number => duration !== null),
  );

  // calculate version change flags
  const runsWithVersionFlags = useGridRunsWithVersionFlags({
    gridRuns,
    showVersionIndicatorMode,
  });

  const { flatNodes } = flattenNodes(dagStructure, openGroupIds);

  const taskNameColumnWidthPx = showGantt ? estimateTaskNameColumnWidthPx(flatNodes) : undefined;

  const taskNameColumnStyles =
    showGantt && taskNameColumnWidthPx !== undefined
      ? {
          flexGrow: 0,
          flexShrink: 0,
          maxW: `${taskNameColumnWidthPx}px`,
          minW: `${taskNameColumnWidthPx}px`,
          width: `${taskNameColumnWidthPx}px`,
        }
      : {
          flexGrow: 1,
          flexShrink: 0,
          minW: "200px",
        };

  const { setMode } = useNavigation({
    onToggleGroup: toggleGroupId,
    runs: gridRuns ?? [],
    tasks: flatNodes,
  });

  const handleRowClick = () => setMode(NavigationModes.TASK);
  const handleCellClick = () => setMode(NavigationModes.TI);
  const handleColumnClick = () => setMode(NavigationModes.RUN);

  const rowVirtualizer = useVirtualizer({
    count: flatNodes.length,
    estimateSize: () => ROW_HEIGHT,
    // @tanstack/react-virtual: pass element resolver inline; hook tracks scroll container via its own subscriptions.
    getScrollElement: () =>
      usesSharedScroll ? (sharedScrollContainerRef?.current ?? null) : scrollContainerRef.current,
    overscan: 5,
    scrollPaddingStart: usesSharedScroll ? GANTT_ROW_OFFSET_PX : GRID_INNER_SCROLL_PADDING_START_PX,
  });

  const virtualItems = rowVirtualizer.getVirtualItems();

  const gridHeaderAndBody = (
    <>
      {/* Grid header, both bgs are needed to hide elements during horizontal and vertical scroll */}
      <Flex bg="bg" display="flex" position="sticky" pt={`${GRID_HEADER_PADDING_PX}px`} top={0} zIndex={2}>
        <Box bg="bg" left={0} position="sticky" zIndex={1} {...taskNameColumnStyles}>
          <Flex flexDirection="column-reverse" height={`${GRID_HEADER_HEIGHT_PX}px`} position="relative">
            {Boolean(gridRuns?.length) && (
              <>
                <DurationTick bottom={`${GRID_HEADER_HEIGHT_PX - 8}px`} duration={max} />
                <DurationTick bottom={`${GRID_HEADER_HEIGHT_PX / 2 - 4}px`} duration={max / 2} />
              </>
            )}
          </Flex>
        </Box>
        {/* Duration bars */}
        <Flex flexDirection="row-reverse" flexShrink={0}>
          <Flex flexShrink={0} position="relative">
            <DurationAxis top={`${GRID_HEADER_HEIGHT_PX}px`} />
            <DurationAxis top={`${GRID_HEADER_HEIGHT_PX / 2}px`} />
            <DurationAxis top="4px" />
            <Flex flexDirection="row-reverse">
              {runsWithVersionFlags?.map((dr) => (
                <Bar
                  key={dr.run_id}
                  max={max}
                  onClick={handleColumnClick}
                  run={dr}
                  showVersionIndicatorMode={showVersionIndicatorMode}
                />
              ))}
            </Flex>
            <GridPaginationButtons
              hasNewerRuns={hasNewerRuns}
              hasOlderRuns={hasOlderRuns}
              isLoading={isLoading}
              latestNotVisible={latestNotVisible}
              onJumpToLatest={onJumpToLatest}
              onNewerRuns={handleNewerRuns}
              onOlderRuns={handleOlderRuns}
            />
          </Flex>
        </Flex>
      </Flex>

      {/* Grid body */}
      <Flex height={`${rowVirtualizer.getTotalSize()}px`} position="relative">
        <Box bg="bg" left={0} position="sticky" zIndex={1} {...taskNameColumnStyles}>
          <TaskNames nodes={flatNodes} onRowClick={handleRowClick} virtualItems={virtualItems} />
        </Box>
        <Flex flexDirection="row-reverse" flexShrink={0}>
          {gridRuns?.map((dr: GridRunsResponse) => (
            <TaskInstancesColumn
              key={dr.run_id}
              nodes={flatNodes}
              onCellClick={handleCellClick}
              run={dr}
              showVersionIndicatorMode={showVersionIndicatorMode}
              tiSummaries={summariesByRunId.get(dr.run_id)}
              virtualItems={virtualItems}
            />
          ))}
        </Flex>
      </Flex>
    </>
  );

  return (
    <Flex
      flexDirection="column"
      flexGrow={showGantt ? 0 : 1}
      flexShrink={showGantt ? 0 : undefined}
      height={showGantt ? undefined : "100%"}
      justifyContent="flex-start"
      position="relative"
      ref={gridRef}
      tabIndex={0}
      w={showGantt ? "fit-content" : "full"}
    >
      {usesSharedScroll ? (
        gridHeaderAndBody
      ) : (
        <Box
          flex={1}
          marginRight={showGantt ? 0 : 1}
          minH={0}
          overflow="auto"
          paddingRight={showGantt ? 0 : 6}
          position="relative"
          ref={scrollContainerRef}
        >
          {gridHeaderAndBody}
        </Box>
      )}
    </Flex>
  );
};
