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
import { Box, Text } from "@chakra-ui/react";
import type { MouseEvent as ReactMouseEvent, ReactNode, RefObject } from "react";
import { useTranslation } from "react-i18next";

import { TimelineBar } from "./TimelineBar";
import { dayjs } from "./dateUtils";
import { buildTimeMarkers, buildWeekItemLayouts } from "./timelineUtils";
import type { TimeMarker, TimeScale, TimelineItem } from "./types";

const WEEKDAYS = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"];

type WeekTimelineProps = {
  readonly chartBodyRef: RefObject<HTMLDivElement | null>;
  readonly chartRootRef: RefObject<HTMLDivElement | null>;
  readonly chartViewportHeight: string;
  readonly hourMarkers: Array<Pick<TimeMarker, "minute" | "position">>;
  readonly items: Array<TimelineItem>;
  readonly onMouseLeave: () => void;
  readonly onMouseMove: (event: ReactMouseEvent<HTMLDivElement>) => void;
  readonly renderTooltip: (item: TimelineItem) => ReactNode;
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
  const { t: translate } = useTranslation();
  const contentHeight = (24 * 60 * 40) / timeScale;
  const timeMarkers = buildTimeMarkers(timeScale);

  return (
    <Box
      borderColor="border.subtle"
      borderRadius="md"
      borderWidth="1px"
      data-testid="time-schedule-week-grid"
      height={chartViewportHeight}
      minHeight="420px"
      overflow="hidden"
      overscrollBehavior="auto"
    >
      <Box data-testid="time-schedule-week-header" overflow="hidden" ref={weekHeaderRef}>
        <Box display="grid" gridTemplateColumns="56px repeat(7, minmax(160px, 1fr))" minWidth="1120px">
          <Box bg="bg.subtle" borderBottomColor="border.subtle" borderBottomWidth="1px" height="40px" />
          {WEEKDAYS.map((weekday) => (
            <Box
              alignItems="center"
              bg="bg.subtle"
              borderBottomColor="border.subtle"
              borderBottomWidth="1px"
              borderLeftColor="border.subtle"
              borderLeftWidth="1px"
              display="flex"
              height="40px"
              justifyContent="center"
              key={weekday}
            >
              <Text fontSize="sm" fontWeight="semibold">
                {translate(`timeSchedule.weekday.${weekday}`)}
              </Text>
            </Box>
          ))}
        </Box>
      </Box>
      <Box
        data-testid="time-schedule-week-body"
        height="calc(100% - 40px)"
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
        <Box display="grid" gridTemplateColumns="56px repeat(7, minmax(160px, 1fr))" minWidth="1120px">
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
              (item) => item.startDate !== null && dayjs(item.startDate).tz(selectedTimezone).day() === day,
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
                    key={item.dagRunId}
                    left={`calc(${(column / columnCount) * 100}% + 2px)`}
                    renderTooltip={renderTooltip}
                    showDagId
                    testId={`time-schedule-week-bar-${item.dagRunId}`}
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
