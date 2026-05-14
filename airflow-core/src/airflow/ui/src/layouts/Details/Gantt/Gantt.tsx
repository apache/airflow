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
import { useRef } from "react";
import type { RefObject } from "react";
import { useParams, useSearchParams } from "react-router-dom";

import { useGanttServiceGetGanttData } from "openapi/queries";
import type { DagRunState, DagRunType } from "openapi/requests/types.gen";
import { useHover } from "src/context/hover";
import { useOpenGroups } from "src/context/openGroups";
import { useTimezone } from "src/context/timezone";
import { NavigationModes, useNavigation } from "src/hooks/navigation";
import {
  GANTT_AXIS_HEIGHT_PX,
  GANTT_ROW_OFFSET_PX,
  GANTT_TOP_PADDING_PX,
} from "src/layouts/Details/Grid/constants";
import { flattenNodes } from "src/layouts/Details/Grid/utils";
import { useGridRuns } from "src/queries/useGridRuns";
import { useGridStructure } from "src/queries/useGridStructure";
import { useGridTiSummariesStream } from "src/queries/useGridTISummaries";
import { isStatePending, useAutoRefresh } from "src/utils";

import { GanttTimeline } from "./GanttTimeline";
import { buildGanttRowSegments, computeGanttTimeRangeMs, transformGanttData } from "./utils";

const GANTT_STANDALONE_VIRTUALIZER_PADDING_START_PX = GANTT_TOP_PADDING_PX + GANTT_AXIS_HEIGHT_PX;

type Props = {
  readonly dagRunState?: DagRunState | undefined;
  readonly limit: number;
  readonly offset?: number;
  readonly runAfterGte?: string | undefined;
  readonly runAfterLte?: string | undefined;
  readonly runType?: DagRunType | undefined;
  readonly sharedScrollContainerRef?: RefObject<HTMLDivElement | null>;
  readonly triggeringUser?: string | undefined;
};

export const Gantt = ({
  dagRunState,
  limit,
  offset,
  runAfterGte,
  runAfterLte,
  runType,
  sharedScrollContainerRef,
  triggeringUser,
}: Props) => {
  const standaloneScrollRef = useRef<HTMLDivElement | null>(null);
  const usesSharedScroll = sharedScrollContainerRef !== undefined;

  const scrollContainerRef = usesSharedScroll ? sharedScrollContainerRef : standaloneScrollRef;

  const { dagId = "", runId = "" } = useParams();
  const [searchParams] = useSearchParams();
  const { openGroupIds, toggleGroupId } = useOpenGroups();
  const { selectedTimezone } = useTimezone();
  const { setHoveredTaskId } = useHover();

  const filterRoot = searchParams.get("root") ?? undefined;
  const includeUpstream = searchParams.get("upstream") === "true";
  const includeDownstream = searchParams.get("downstream") === "true";
  const depthParam = searchParams.get("depth");
  const depth = depthParam !== null && depthParam !== "" ? parseInt(depthParam, 10) : undefined;

  const { data: gridRuns, isLoading: runsLoading } = useGridRuns({
    dagRunState,
    limit,
    offset,
    runAfterGte,
    runAfterLte,
    runType,
    triggeringUser,
  });
  const { data: dagStructure, isLoading: structureLoading } = useGridStructure({
    dagRunState,
    depth,
    includeDownstream,
    includeUpstream,
    limit,
    root: filterRoot,
    runType,
    triggeringUser,
  });
  const selectedRun = gridRuns?.find((run) => run.run_id === runId);
  const refetchInterval = useAutoRefresh({ dagId });

  const { summariesByRunId } = useGridTiSummariesStream({
    dagId,
    runIds: runId && selectedRun ? [runId] : [],
    states: selectedRun ? [selectedRun.state] : [],
  });
  const gridTiSummaries = summariesByRunId.get(runId);
  const summariesLoading = Boolean(runId && selectedRun && !summariesByRunId.has(runId));

  const { data: ganttData, isLoading: ganttLoading } = useGanttServiceGetGanttData(
    { dagId, runId },
    undefined,
    {
      enabled: Boolean(dagId) && Boolean(runId) && Boolean(selectedRun),
      refetchInterval: (query) =>
        query.state.data?.task_instances.some((ti) => isStatePending(ti.state)) ? refetchInterval : false,
    },
  );

  const { flatNodes } = flattenNodes(dagStructure, openGroupIds);

  const { setMode } = useNavigation({
    onToggleGroup: toggleGroupId,
    runs: gridRuns ?? [],
    tasks: flatNodes,
  });

  const isLoading = runsLoading || structureLoading || summariesLoading || ganttLoading;

  const allTries = ganttData?.task_instances ?? [];
  const gridSummaries = gridTiSummaries?.task_instances ?? [];

  const ganttDataItems =
    isLoading || runId === "" ? [] : transformGanttData({ allTries, flatNodes, gridSummaries });

  const rowSegments = buildGanttRowSegments(flatNodes, ganttDataItems);

  const { maxMs, minMs } = computeGanttTimeRangeMs({
    ganttItems: ganttDataItems,
    selectedRun,
    selectedTimezone,
  });

  const virtualizerScrollPaddingStart = usesSharedScroll
    ? GANTT_ROW_OFFSET_PX
    : GANTT_STANDALONE_VIRTUALIZER_PADDING_START_PX;

  if (runId === "" || (!isLoading && !selectedRun)) {
    return undefined;
  }

  const handleStandaloneMouseLeave = () => {
    setHoveredTaskId(undefined);
  };

  const timeline =
    Boolean(selectedRun) && dagId ? (
      <GanttTimeline
        dagId={dagId}
        flatNodes={flatNodes}
        ganttDataItems={ganttDataItems}
        gridSummaries={gridSummaries}
        maxMs={maxMs}
        minMs={minMs}
        onSegmentClick={() => setMode(NavigationModes.TI)}
        rowSegments={rowSegments}
        runId={runId}
        scrollContainerRef={scrollContainerRef}
        virtualizerScrollPaddingStart={virtualizerScrollPaddingStart}
      />
    ) : undefined;

  if (usesSharedScroll) {
    return (
      <Flex flex={1} flexDirection="column" maxW="100%" minW={0} overflow="clip" pt={0}>
        {timeline}
      </Flex>
    );
  }

  return (
    <Flex flex={1} flexDirection="column" maxW="100%" minH={0} minW={0} overflow="hidden">
      <Box
        flex={1}
        minH={0}
        minW={0}
        onMouseLeave={handleStandaloneMouseLeave}
        overflowX="hidden"
        overflowY="auto"
        ref={standaloneScrollRef}
      >
        {timeline}
      </Box>
    </Flex>
  );
};
