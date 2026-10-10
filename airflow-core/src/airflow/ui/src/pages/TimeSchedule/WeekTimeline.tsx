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

import { Box, Text } from "@chakra-ui/react";
import dayjs from "dayjs";
import { useTranslation } from "react-i18next";

import type { TimeScheduleItem } from "openapi/requests/types.gen";

import { TimelineBar } from "./TimelineBar";
import {
  WEEK_LABEL_LINE_HEIGHT_PX,
  DAY_MINUTES,
  WEEK_HEADER_HEIGHT_PX,
  WEEK_TIME_LABEL_WIDTH_PX,
  WEEK_DAY_MIN_WIDTH_PX,
  TIMELINE_MIN_HEIGHT_PX,
  TIME_SLOT_SIZE_PX,
} from "./constants";
import { buildTimeMarkers, buildWeekItemLayouts } from "./timelineUtils";
import type { TimeMarker, TimeScale } from "./types";

const WEEKDAYS = ["sunday", "monday", "tuesday", "wednesday", "thursday", "friday", "saturday"];

type WeekTimelineProps = {
  readonly chartBodyRef: RefObject<HTMLDivElement | null>;
  readonly chartRootRef: RefObject<HTMLDivElement | null>;
  readonly chartViewportHeight: string;
  readonly hourMarkers: Array<Pick<TimeMarker, "minute" | "position">>;
  readonly items: Array<TimeScheduleItem>;
  readonly onMouseLeave: () => void;
  readonly onMouseMove: (event: ReactMouseEvent<HTMLDivElement>) => void;
  readonly renderTooltip: (item: TimeScheduleItem) => ReactNode;
  readonly selectedTimezone: string;
  readonly timeScale: TimeScale;
  readonly weekHeaderRef: RefObject<HTMLDivElement | null>;
};

export const WeekTimeline = ({
  chartBodyRef,
  chartRootRef,
  chartViewportHeight,
  hourMarkers,
  items,
  onMouseLeave,
  onMouseMove,
  renderTooltip,
  selectedTimezone,
  timeScale,
  weekHeaderRef,
}: WeekTimelineProps) => {
  const { t: translate } = useTranslation("dag");
  const contentHeight = (DAY_MINUTES * TIME_SLOT_SIZE_PX) / timeScale;
  const timeMarkers = buildTimeMarkers(timeScale);

  return (
    <Box
      borderColor="border.subtle"
      borderRadius="md"
      borderWidth="1px"
      data-testid="time-schedule-week-grid"
      height={chartViewportHeight}
      minHeight={`${TIMELINE_MIN_HEIGHT_PX}px`}
      overflow="hidden"
      overscrollBehavior="auto"
    >
      <Box data-testid="time-schedule-week-header" overflow="hidden" ref={weekHeaderRef}>
        <Box
          display="grid"
          gridTemplateColumns={`${WEEK_TIME_LABEL_WIDTH_PX}px repeat(7, minmax(${WEEK_DAY_MIN_WIDTH_PX}px, 1fr))`}
          minWidth={`${WEEK_TIME_LABEL_WIDTH_PX + 7 * WEEK_DAY_MIN_WIDTH_PX}px`}
        >
          <Box
            bg="bg.subtle"
            borderBottomColor="border.subtle"
            borderBottomWidth="1px"
            height={`${WEEK_HEADER_HEIGHT_PX}px`}
          />
          {WEEKDAYS.map((weekday) => (
            <Box
              alignItems="center"
              bg="bg.subtle"
              borderBottomColor="border.subtle"
              borderBottomWidth="1px"
              borderLeftColor="border.subtle"
              borderLeftWidth="1px"
              display="flex"
              height={`${WEEK_HEADER_HEIGHT_PX}px`}
              justifyContent="center"
              key={weekday}
            >
              <Text fontSize="sm" fontWeight="semibold">
                {translate(`calendar.weekdays.${weekday}`)}
              </Text>
            </Box>
          ))}
        </Box>
      </Box>
      <Box
        data-testid="time-schedule-week-body"
        height={`calc(100% - ${WEEK_HEADER_HEIGHT_PX}px)`}
        minHeight={0}
        onMouseDown={() => chartRootRef.current?.focus({ preventScroll: true })}
        onMouseLeave={onMouseLeave}
        onMouseMove={onMouseMove}
        overflowX="auto"
        overflowY="auto"
        overscrollBehavior="auto"
        pt="10px"
        ref={chartBodyRef}
      >
        <Box
          display="grid"
          gridTemplateColumns={`${WEEK_TIME_LABEL_WIDTH_PX}px repeat(7, minmax(${WEEK_DAY_MIN_WIDTH_PX}px, 1fr))`}
          minWidth={`${WEEK_TIME_LABEL_WIDTH_PX + 7 * WEEK_DAY_MIN_WIDTH_PX}px`}
        >
          <Box height={`${contentHeight}px`} position="relative">
            {timeMarkers.map(({ label, minute, position }) => (
              <Text
                color="fg.muted"
                fontSize="xs"
                key={minute}
                position="absolute"
                right={2}
                top={`${position}%`}
                transform="translateY(-50%)"
              >
                {label}
              </Text>
            ))}
          </Box>
          {WEEKDAYS.map((_, day) => {
            const dayItems = items.filter(
              (item) => item.start_date !== null && dayjs(item.start_date).tz(selectedTimezone).day() === day,
            );
            const layouts = buildWeekItemLayouts({ contentHeight, items: dayItems, selectedTimezone });

            return (
              <Box
                borderLeftColor="border.subtle"
                borderLeftWidth="1px"
                height={`${contentHeight}px`}
                key={WEEKDAYS[day]}
                position="relative"
              >
                {hourMarkers.map(({ minute, position }) => (
                  <Box
                    borderTopColor="border.emphasized"
                    borderTopStyle={minute % 360 === 0 ? "solid" : "dotted"}
                    borderTopWidth="1px"
                    key={minute}
                    left={0}
                    position="absolute"
                    right={0}
                    top={`${position}%`}
                  />
                ))}
                {layouts.map(({ column, columnCount, height, item, top }) => (
                  <TimelineBar
                    height={`${height}px`}
                    item={item}
                    key={item.dag_run_id}
                    labelLineClamp={Math.max(1, Math.floor(height / WEEK_LABEL_LINE_HEIGHT_PX))}
                    left={`calc(${(column / columnCount) * 100}% + 2px)`}
                    renderTooltip={renderTooltip}
                    selectedTimezone={selectedTimezone}
                    showDagLabel
                    testId={`time-schedule-week-bar-${item.dag_run_id}`}
                    top={`${top}px`}
                    width={`calc(${100 / columnCount}% - 4px)`}
                  />
                ))}
              </Box>
            );
          })}
        </Box>
      </Box>
    </Box>
  );
};
