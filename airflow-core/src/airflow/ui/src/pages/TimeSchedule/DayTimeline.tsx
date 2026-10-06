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
import type { MouseEvent as ReactMouseEvent, ReactNode, RefObject } from "react";

import { Box, Button, Text } from "@chakra-ui/react";
import dayjs from "dayjs";
import { useTranslation } from "react-i18next";
import { FiArrowDown, FiArrowUp } from "react-icons/fi";

import type { TimeScheduleItem } from "openapi/requests/types.gen";

import { RouterLink } from "src/system-components";

import { TimelineBar } from "./TimelineBar";
import {
  DAY_LABEL_WIDTH_PX,
  DAY_HEADER_HEIGHT_PX,
  TIMELINE_HORIZONTAL_PADDING,
  TIMELINE_MIN_HEIGHT_PX,
  DAY_LANE_HEIGHT_PX,
} from "./constants";
import { getPosition, getTimelineBarLeft, getVisualDurationWidth } from "./timelineUtils";
import type { DayRowLayout, RowSortMode, TimeMarker } from "./types";

type DayTimelineProps = {
  readonly chartBodyRef: RefObject<HTMLDivElement | null>;
  readonly chartContentHeight: number;
  readonly chartMinWidth?: string;
  readonly chartRootRef: RefObject<HTMLDivElement | null>;
  readonly chartViewportHeight: string;
  readonly headerRowRef: RefObject<HTMLDivElement | null>;
  readonly hourMarkers: Array<Pick<TimeMarker, "minute" | "position">>;
  readonly layouts: Array<DayRowLayout>;
  readonly onCycleSort: () => void;
  readonly onMouseLeave: () => void;
  readonly onMouseMove: (event: ReactMouseEvent<HTMLDivElement>) => void;
  readonly renderTooltip: (item: TimeScheduleItem) => ReactNode;
  readonly rowSortMode: RowSortMode;
  readonly scrollRegionRef: RefObject<HTMLDivElement | null>;
  readonly selectedTimezone: string;
  readonly timeLabelStep: number;
  readonly timelineMinWidth?: string;
  readonly timeMarkers: Array<TimeMarker>;
};

export const DayTimeline = ({
  chartBodyRef,
  chartContentHeight,
  chartMinWidth,
  chartRootRef,
  chartViewportHeight,
  headerRowRef,
  hourMarkers,
  layouts,
  onCycleSort,
  onMouseLeave,
  onMouseMove,
  renderTooltip,
  rowSortMode,
  scrollRegionRef,
  selectedTimezone,
  timeLabelStep,
  timelineMinWidth,
  timeMarkers,
}: DayTimelineProps) => {
  const { i18n, t: translate } = useTranslation();

  return (
    <Box
      borderColor="border.subtle"
      borderRadius="md"
      borderWidth="1px"
      data-testid="time-schedule-day-grid"
      height={chartViewportHeight}
      minHeight={`${TIMELINE_MIN_HEIGHT_PX}px`}
      overflow="hidden"
    >
      <Box position="sticky" top={0} zIndex={5}>
        <Box display="grid" gridTemplateColumns={`${DAY_LABEL_WIDTH_PX}px minmax(0, 1fr)`}>
          <Box bg="bg.subtle" borderBottomColor="border.subtle" borderBottomWidth="1px" p={3}>
            <Button
              aria-label={`Sort Dag ID: ${rowSortMode}`}
              color="fg.muted"
              fontSize="sm"
              fontWeight="normal"
              gap={1}
              onClick={onCycleSort}
              p={0}
              variant="plain"
            >
              {translate("dagId")}
              {rowSortMode === "dagIdAscending" ? <FiArrowUp /> : null}
              {rowSortMode === "dagIdDescending" ? <FiArrowDown /> : null}
            </Button>
          </Box>
          <Box
            bg="bg.panel"
            borderBottomColor="border.subtle"
            borderBottomWidth="1px"
            data-testid="time-schedule-header-row"
            minHeight={`${DAY_HEADER_HEIGHT_PX}px`}
            onMouseDown={() => chartRootRef.current?.focus({ preventScroll: true })}
            onMouseLeave={onMouseLeave}
            onMouseMove={onMouseMove}
            overflowX="hidden"
            py={3}
            ref={headerRowRef}
          >
            <Box
              height="24px"
              minWidth={timelineMinWidth}
              mx={`${TIMELINE_HORIZONTAL_PADDING / 2}px`}
              position="relative"
              width={`calc(100% - ${TIMELINE_HORIZONTAL_PADDING}px)`}
            >
              {timeMarkers.map(({ label, minute, position }, index) =>
                index === 0 || index === timeMarkers.length - 1 || index % timeLabelStep === 0 ? (
                  <Text
                    bg="bg.panel"
                    color="fg.muted"
                    fontSize="xs"
                    key={minute}
                    left={`${position}%`}
                    position="absolute"
                    px={1}
                    transform="translateX(-50%)"
                    whiteSpace="nowrap"
                  >
                    {label}
                  </Text>
                ) : null,
              )}
            </Box>
          </Box>
        </Box>
      </Box>
      <Box
        data-testid="time-schedule-scroll-region"
        height={`calc(100% - ${DAY_HEADER_HEIGHT_PX}px)`}
        minHeight={0}
        overflowX="hidden"
        overflowY="auto"
        ref={scrollRegionRef}
      >
        <Box display="grid" gridTemplateColumns={`${DAY_LABEL_WIDTH_PX}px minmax(0, 1fr)`}>
          <Box
            bg="bg.subtle"
            borderRightColor="border.subtle"
            borderRightWidth="1px"
            data-testid="time-schedule-rows-body"
          >
            {layouts.map(({ height, row }) => (
              <Box
                alignItems="center"
                borderBottomColor="border.subtle"
                borderBottomWidth="1px"
                display="flex"
                height={`${height}px`}
                key={row.dag_id}
                p={3}
              >
                <RouterLink style={{ minWidth: 0, width: "100%" }} to={`/dags/${row.dag_id}`}>
                  <Text
                    display="block"
                    fontSize="sm"
                    fontWeight="medium"
                    overflow="hidden"
                    textOverflow="ellipsis"
                    whiteSpace="nowrap"
                  >
                    {row.dag_display_name}
                  </Text>
                </RouterLink>
              </Box>
            ))}
          </Box>
          <Box
            data-testid="time-schedule-chart-body"
            minHeight={0}
            onMouseDown={() => chartRootRef.current?.focus({ preventScroll: true })}
            onMouseLeave={onMouseLeave}
            onMouseMove={onMouseMove}
            overflowX="auto"
            overflowY="hidden"
            ref={chartBodyRef}
          >
            <Box minHeight={`${chartContentHeight}px`} minWidth={chartMinWidth} position="relative">
              <Box
                height={`${chartContentHeight}px`}
                mx={`${TIMELINE_HORIZONTAL_PADDING / 2}px`}
                position="relative"
                pt={2}
              >
                {hourMarkers.map(({ minute, position }) => (
                  <Box data-testid={`time-schedule-grid-line-${minute}`} key={minute}>
                    <Box
                      borderLeftColor="border.emphasized"
                      borderLeftStyle={minute % 360 === 0 ? "solid" : "dotted"}
                      borderLeftWidth="1px"
                      bottom={0}
                      left={`${position}%`}
                      position="absolute"
                      top={0}
                    />
                  </Box>
                ))}
                {layouts.flatMap(({ items, top }) =>
                  items.map(({ item, lane }) => {
                    const start =
                      item.start_date === null ? null : dayjs(item.start_date).tz(selectedTimezone);
                    const end = item.end_date === null ? start : dayjs(item.end_date).tz(selectedTimezone);
                    const dayStart = start?.startOf("day");
                    const startPosition = start && dayStart ? getPosition(start, dayStart) : 0;
                    const endPosition = end && dayStart ? getPosition(end, dayStart) : startPosition;
                    const barWidth = getVisualDurationWidth(item.duration_ms, i18n.language);

                    return (
                      <Box
                        key={`${item.dag_id}-${item.dag_run_id}`}
                        left={0}
                        position="absolute"
                        right={0}
                        top={`${top + lane * DAY_LANE_HEIGHT_PX + 16}px`}
                      >
                        <TimelineBar
                          height="12px"
                          item={item}
                          left={getTimelineBarLeft(Math.min(startPosition, endPosition), barWidth)}
                          renderTooltip={renderTooltip}
                          testId={`time-schedule-run-bar-${item.dag_run_id}`}
                          width={barWidth}
                        />
                      </Box>
                    );
                  }),
                )}
              </Box>
            </Box>
          </Box>
        </Box>
      </Box>
    </Box>
  );
};
