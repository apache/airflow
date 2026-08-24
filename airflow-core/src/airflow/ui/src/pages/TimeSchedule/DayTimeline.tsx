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
import { Box, Button, Text } from "@chakra-ui/react";
import type { MouseEvent as ReactMouseEvent, ReactNode, RefObject } from "react";
import { useTranslation } from "react-i18next";
import { FiArrowDown, FiArrowUp } from "react-icons/fi";

import { RouterLink } from "src/components/ui/RouterLink";

import { TimelineBar } from "./TimelineBar";
import { dayjs } from "./dateUtils";
import { getPosition, getVisualDurationWidth } from "./timelineUtils";
import type { DayRowLayout, RowSortMode, TimeMarker, TimelineItem } from "./types";

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
  readonly renderTooltip: (item: TimelineItem) => ReactNode;
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
  const { t: translate } = useTranslation();

  return (
    <Box
      borderColor="border.subtle"
      borderRadius="md"
      borderWidth="1px"
      data-testid="time-schedule-day-grid"
      height={chartViewportHeight}
      minHeight="420px"
      overflow="hidden"
    >
      <Box position="sticky" top={0} zIndex={5}>
        <Box display="grid" gridTemplateColumns="220px minmax(0, 1fr)">
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
            minHeight="48px"
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
              mx="20px"
              position="relative"
              width="calc(100% - 40px)"
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
        height="calc(100% - 48px)"
        minHeight={0}
        overflowX="hidden"
        overflowY="auto"
        ref={scrollRegionRef}
      >
        <Box display="grid" gridTemplateColumns="220px minmax(0, 1fr)">
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
                key={row.dagId}
                p={3}
              >
                <RouterLink style={{ minWidth: 0, width: "100%" }} to={`/dags/${row.dagId}`}>
                  <Text
                    display="block"
                    fontSize="sm"
                    fontWeight="medium"
                    overflow="hidden"
                    textOverflow="ellipsis"
                    whiteSpace="nowrap"
                  >
                    {row.label}
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
              <Box height={`${chartContentHeight}px`} mx="20px" position="relative" pt={2}>
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
                    const start = item.startDate === null ? null : dayjs(item.startDate).tz(selectedTimezone);
                    const end = item.endDate === null ? start : dayjs(item.endDate).tz(selectedTimezone);
                    const dayStart = start?.startOf("day");
                    const startPosition = start && dayStart ? getPosition(start, dayStart) : 0;
                    const endPosition = end && dayStart ? getPosition(end, dayStart) : startPosition;
                    const width = Math.max(1.5, Math.abs(endPosition - startPosition));
                    const barWidth = getVisualDurationWidth(item.durationMs);

                    return (
                      <Box
                        key={`${item.dagId}-${item.dagRunId}`}
                        left={0}
                        position="absolute"
                        right={0}
                        top={`${top + lane * 20 + 16}px`}
                      >
                        <TimelineBar
                          height="12px"
                          item={item}
                          left={`${Math.min(Math.min(startPosition, endPosition), 100 - width)}%`}
                          renderTooltip={renderTooltip}
                          testId={`time-schedule-run-bar-${item.dagRunId}`}
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
