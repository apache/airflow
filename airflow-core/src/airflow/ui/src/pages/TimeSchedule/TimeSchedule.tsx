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
import { useState } from "react";

import { Box, Flex, Text, VStack } from "@chakra-ui/react";
import { useTranslation } from "react-i18next";
import { useLocalStorage } from "usehooks-ts";

import { ErrorAlert } from "src/components/ErrorAlert";

import {
  TIME_SCHEDULE_AGGREGATION_MODE_KEY,
  TIME_SCHEDULE_DAG_RUN_LIMIT_KEY,
  TIME_SCHEDULE_VIEW_MODE_KEY,
} from "src/constants/localStorage";
import { useTimezone } from "src/context/timezone";
import { useTimeScheduleData } from "src/queries/useTimeScheduleData";
import { useDocumentTitle } from "src/utils";

import { DayTimeline } from "./DayTimeline";
import { TimeScheduleControls } from "./TimeScheduleControls";
import { TimeScheduleViewControls } from "./TimeScheduleViewControls";
import { TimelineTooltip } from "./TimelineTooltip";
import { WeekTimeline } from "./WeekTimeline";
import {
  TIMELINE_HORIZONTAL_PADDING,
  DAY_GRID_MIN_HEIGHT_PX,
  DAY_CONTENT_MIN_HEIGHT_PX,
  DAY_GRID_BOTTOM_PADDING_PX,
  DAY_HEADER_HEIGHT_PX,
} from "./constants";
import { buildDayRowLayouts, buildTimelineRows } from "./timelineUtils";
import type { AggregationMode, DagRunLimit, RowSortMode, ViewMode } from "./types";
import { useTimeScheduleFilters } from "./useTimeScheduleFilters";
import { useTimelineZoom } from "./useTimelineZoom";

const CHART_VIEWPORT_HEIGHT = "calc(100dvh - 160px)";

export const TimeSchedule = () => {
  const { i18n, t: translate } = useTranslation();
  const { selectedTimezone } = useTimezone();
  const [viewMode, setViewMode] = useLocalStorage<ViewMode>(TIME_SCHEDULE_VIEW_MODE_KEY, "day");
  const [aggregationMode, setAggregationMode] = useLocalStorage<AggregationMode>(
    TIME_SCHEDULE_AGGREGATION_MODE_KEY,
    "mean",
  );
  const [dagRunLimit, setDagRunLimit] = useLocalStorage<DagRunLimit>(TIME_SCHEDULE_DAG_RUN_LIMIT_KEY, 200);
  const [rowSortMode, setRowSortMode] = useState<RowSortMode>("dagIdAscending");
  const zoom = useTimelineZoom(viewMode);

  useDocumentTitle(translate("timeSchedule.title"));

  const { controls, streamQuery } = useTimeScheduleFilters({
    aggregationMode,
    dagRunLimit,
    selectedTimezone,
    timeScale: zoom.timeScale,
    viewMode,
  });
  const { dagRunCount, error, isLoading, timelineItems } = useTimeScheduleData(streamQuery);
  const dayRows =
    viewMode === "day" ? buildTimelineRows({ items: timelineItems, rowSortMode, selectedTimezone }) : [];
  const dayRowLayouts = buildDayRowLayouts({
    locale: i18n.language,
    rows: dayRows,
    selectedTimezone,
    timelineWidth: zoom.chartWidth - TIMELINE_HORIZONTAL_PADDING,
  });
  const dayGridHeight = Math.max(
    DAY_GRID_MIN_HEIGHT_PX,
    dayRowLayouts.reduce((height, row) => height + row.height, 0) + DAY_GRID_BOTTOM_PADDING_PX,
  );
  const chartContentHeight = Math.max(DAY_CONTENT_MIN_HEIGHT_PX, dayGridHeight - DAY_HEADER_HEIGHT_PX);
  const chartMinWidth = zoom.timeScale === 60 ? undefined : `${zoom.chartWidth}px`;
  const timelineMinWidth =
    zoom.timeScale === 60 ? undefined : `${zoom.chartWidth - TIMELINE_HORIZONTAL_PADDING}px`;
  const cycleRowSortMode = () => {
    setRowSortMode((current) =>
      current === "dagIdAscending"
        ? "dagIdDescending"
        : current === "dagIdDescending"
          ? "startTime"
          : "dagIdAscending",
    );
  };
  const renderTimelineTooltip = (item: (typeof timelineItems)[number]) => (
    <TimelineTooltip item={item} selectedTimezone={selectedTimezone} />
  );

  return (
    <VStack align="stretch" gap={4}>
      <TimeScheduleControls {...controls} onViewModeChange={setViewMode} viewMode={viewMode} />

      <Box
        bg="bg.panel"
        borderColor="border.subtle"
        borderRadius="md"
        borderWidth="1px"
        data-testid="time-schedule-chart"
        onMouseLeave={zoom.onChartMouseLeave}
        onMouseMove={zoom.onChartMouseMove}
        overscrollBehavior="auto"
        p={4}
        ref={zoom.chartRootRef}
        tabIndex={0}
      >
        <VStack align="stretch" gap={3} height="100%" minHeight={0}>
          <Flex align="center" gap={4} justify="space-between" wrap="wrap">
            <Text color="fg.muted" fontSize="sm">
              {isLoading ? null : `${dagRunCount} ${translate("dagRun", { count: dagRunCount })}`}
            </Text>
            <TimeScheduleViewControls
              aggregationMode={aggregationMode}
              dagRunLimit={dagRunLimit}
              onAggregationModeChange={setAggregationMode}
              onDagRunLimitChange={setDagRunLimit}
              onZoomIn={zoom.zoomIn}
              onZoomOut={zoom.zoomOut}
              timeScale={zoom.timeScale}
              zoomInDisabled={zoom.zoomInDisabled}
              zoomOutDisabled={zoom.zoomOutDisabled}
            />
          </Flex>
          <ErrorAlert error={error} />

          {viewMode === "day" ? (
            <DayTimeline
              chartBodyRef={zoom.chartBodyRef}
              chartContentHeight={chartContentHeight}
              chartMinWidth={chartMinWidth}
              chartRootRef={zoom.chartRootRef}
              chartViewportHeight={CHART_VIEWPORT_HEIGHT}
              headerRowRef={zoom.headerRowRef}
              hourMarkers={zoom.hourMarkers}
              layouts={dayRowLayouts}
              onCycleSort={cycleRowSortMode}
              onMouseLeave={zoom.onChartMouseLeave}
              onMouseMove={zoom.onChartMouseMove}
              renderTooltip={renderTimelineTooltip}
              rowSortMode={rowSortMode}
              scrollRegionRef={zoom.scrollRegionRef}
              selectedTimezone={selectedTimezone}
              timeLabelStep={zoom.timeLabelStep}
              timelineMinWidth={timelineMinWidth}
              timeMarkers={zoom.timeMarkers}
            />
          ) : (
            <WeekTimeline
              chartBodyRef={zoom.chartBodyRef}
              chartRootRef={zoom.chartRootRef}
              chartViewportHeight={CHART_VIEWPORT_HEIGHT}
              hourMarkers={zoom.hourMarkers}
              items={timelineItems}
              onMouseLeave={zoom.onChartMouseLeave}
              onMouseMove={zoom.onChartMouseMove}
              renderTooltip={renderTimelineTooltip}
              selectedTimezone={selectedTimezone}
              timeScale={zoom.timeScale}
              weekHeaderRef={zoom.weekHeaderRef}
            />
          )}
        </VStack>
      </Box>
    </VStack>
  );
};
