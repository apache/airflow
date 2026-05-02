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
import { Badge, Box, Flex, Text } from "@chakra-ui/react";
import { useVirtualizer } from "@tanstack/react-virtual";
import dayjs from "dayjs";
import type { RefObject } from "react";
import { Fragment, useLayoutEffect, useRef, useState } from "react";
import { Link, useLocation, useParams } from "react-router-dom";

import type { LightGridTaskInstanceSummary } from "openapi/requests/types.gen";
import { StateIcon } from "src/components/StateIcon";
import TaskInstanceTooltip from "src/components/TaskInstanceTooltip";
import { useHover } from "src/context/hover";
import {
  GANTT_AXIS_HEIGHT_PX,
  GANTT_TOP_PADDING_PX,
  ROW_HEIGHT,
  TASK_BAR_HEIGHT_PX,
} from "src/layouts/Details/Grid/constants";
import type { GridTask } from "src/layouts/Details/Grid/utils";

import {
  type GanttDataItem,
  GANTT_TIME_AXIS_TICK_COUNT,
  buildGanttTimeAxisTicks,
  buildMaxTryByTaskId,
  getGanttSegmentTo,
  gridSummariesToTaskIdMap,
} from "./utils";

/** Size of the state icon rendered inside each Gantt bar (px). The minimum bar width is derived
 *  from this value so the icon is never clipped when a task has a very short duration. */
const GANTT_STATE_ICON_SIZE_PX = 10;
const MIN_BAR_WIDTH_PX = GANTT_STATE_ICON_SIZE_PX;

/** Scheduled and queued bars narrower than this are not rendered — they would be invisible anyway
 *  and their presence would suppress the rounded left edge of the adjacent execution bar. */
const MIN_SEGMENT_RENDER_PX = 5;

/** Minimum horizontal gap (px) between time-axis labels before one is dropped. */
const MIN_TICK_SPACING_PX = 80;

/** Short mark above the axis bottom border, aligned with each timestamp. */
const GANTT_AXIS_TICK_HEIGHT_PX = 6;

type Props = {
  readonly dagId: string;
  readonly flatNodes: Array<GridTask>;
  readonly ganttDataItems: Array<GanttDataItem>;
  readonly gridSummaries: Array<LightGridTaskInstanceSummary>;
  readonly maxMs: number;
  readonly minMs: number;
  readonly onSegmentClick?: () => void;
  readonly rowSegments: Array<Array<GanttDataItem>>;
  readonly runId: string;
  readonly scrollContainerRef: RefObject<HTMLDivElement | null>;
  /** scrollPaddingStart for @tanstack/react-virtual (116 standalone, 180 with shared outer padding). */
  readonly virtualizerScrollPaddingStart: number;
};

const toTooltipSummary = (
  segment: GanttDataItem,
  node: GridTask,
  gridSummary: LightGridTaskInstanceSummary | undefined,
) => {
  if (gridSummary !== undefined && (node.isGroup ?? node.is_mapped)) {
    return gridSummary;
  }

  return {
    child_states: null,
    max_end_date: dayjs(segment.x[1]).toISOString(),
    min_start_date: dayjs(segment.x[0]).toISOString(),
    state: segment.state ?? null,
    task_display_name: segment.y,
    task_id: segment.taskId,
    try_number: segment.tryNumber,
    ...(segment.tryNumber === undefined
      ? {}
      : {
          queued_when: segment.queued_when,
          scheduled_when: segment.scheduled_when,
        }),
  };
};

export const GanttTimeline = ({
  dagId,
  flatNodes,
  ganttDataItems,
  gridSummaries,
  maxMs,
  minMs,
  onSegmentClick,
  rowSegments,
  runId,
  scrollContainerRef,
  virtualizerScrollPaddingStart,
}: Props) => {
  const location = useLocation();
  const { groupId: selectedGroupId, taskId: selectedTaskId } = useParams();
  const { hoveredTaskId, setHoveredTaskId } = useHover();
  const [bodyWidthPx, setBodyWidthPx] = useState(0);
  const bodyRef = useRef<HTMLDivElement | null>(null);

  useLayoutEffect(() => {
    const el = bodyRef.current;

    if (el === null) {
      return undefined;
    }

    const ro = new ResizeObserver((entries) => {
      const nextWidth = entries[0]?.contentRect.width;

      if (nextWidth !== undefined) {
        setBodyWidthPx(nextWidth);
      }
    });

    ro.observe(el);

    return () => {
      ro.disconnect();
    };
  }, []);

  const summaryByTaskId = gridSummariesToTaskIdMap(gridSummaries);
  // Precompute max try per task once (O(n)) so getGanttSegmentTo can do O(1) lookups.
  const maxTryByTaskId = buildMaxTryByTaskId(ganttDataItems);
  const spanMs = Math.max(1, maxMs - minMs);

  // Derive tick count from available width so labels never overlap.
  // Each "HH:MM:SS" label is ~8 chars at font-size xs; allow MIN_TICK_SPACING_PX per tick.
  const tickCount =
    bodyWidthPx > 0 ? Math.max(2, Math.floor(bodyWidthPx / MIN_TICK_SPACING_PX)) : GANTT_TIME_AXIS_TICK_COUNT;
  const timeTicks = buildGanttTimeAxisTicks(minMs, maxMs, tickCount);

  const rowVirtualizer = useVirtualizer({
    count: flatNodes.length,
    estimateSize: () => ROW_HEIGHT,
    // @tanstack/react-virtual: scroll container ref; the hook subscribes to this element's scroll/resize.
    getScrollElement: () => scrollContainerRef.current,
    overscan: 5,
    scrollPaddingStart: virtualizerScrollPaddingStart,
  });

  const virtualItems = rowVirtualizer.getVirtualItems();

  // Hoist out of the per-segment render loop — location.pathname and location.search are
  // constant within a single render, so parsing them once avoids O(segments) string parsing.
  const { pathname, search } = location;
  const baseSearchParams = new URLSearchParams(search);

  const segmentLayout = (segment: GanttDataItem) => {
    const leftPct = ((segment.x[0] - minMs) / spanMs) * 100;
    const widthPct = ((segment.x[1] - segment.x[0]) / spanMs) * 100;
    const widthPx = (widthPct / 100) * bodyWidthPx;
    const minBoost = widthPx < MIN_BAR_WIDTH_PX && bodyWidthPx > 0 ? MIN_BAR_WIDTH_PX - widthPx : 0;
    const widthPctAdjusted = bodyWidthPx > 0 ? ((widthPx + minBoost) / bodyWidthPx) * 100 : widthPct;

    return {
      leftPct,
      widthPct: Math.min(widthPctAdjusted, 100 - leftPct),
      widthPx,
    };
  };

  return (
    <Box
      maxW="100%"
      minW={0}
      overflow="clip"
      position="relative"
      style={{ isolation: "isolate" }}
      w="100%"
      zIndex={0}
    >
      <Flex
        bg="bg"
        flexDirection="column"
        flexShrink={0}
        h={`${GANTT_TOP_PADDING_PX + GANTT_AXIS_HEIGHT_PX}px`}
        position="sticky"
        px={0}
        top={0}
        zIndex={3}
      >
        <Box aria-hidden flexShrink={0} h={`${GANTT_TOP_PADDING_PX}px`} />
        <Box
          borderBottomWidth={1}
          borderColor="border"
          flexShrink={0}
          h={`${GANTT_AXIS_HEIGHT_PX}px`}
          position="relative"
          w="100%"
        >
          {timeTicks.map(({ label, labelAlign, leftPct }) => (
            <Fragment key={`gantt-time-tick-${leftPct}`}>
              <Box
                aria-hidden
                borderColor="border"
                borderLeftWidth={1}
                bottom={0}
                h={`${GANTT_AXIS_TICK_HEIGHT_PX}px`}
                left={`calc(${leftPct}% - 0.25px)`}
                position="absolute"
              />
              <Text
                bottom={`${GANTT_AXIS_TICK_HEIGHT_PX + 2}px`}
                color="border.emphasized"
                fontSize="xs"
                left={`${leftPct}%`}
                lineHeight={1}
                position="absolute"
                textAlign={labelAlign}
                transform={
                  labelAlign === "left"
                    ? "translateX(0)"
                    : labelAlign === "right"
                      ? "translateX(-100%)"
                      : "translateX(-50%)"
                }
                whiteSpace="nowrap"
              >
                {label}
              </Text>
            </Fragment>
          ))}
        </Box>
      </Flex>

      <Box maxW="100%" minW={0} overflow="hidden" position="relative" ref={bodyRef} w="100%" zIndex={0}>
        <Box h={`${rowVirtualizer.getTotalSize()}px`} maxW="100%" position="relative" w="100%">
          <Box
            aria-hidden
            h="100%"
            left={0}
            pointerEvents="none"
            position="absolute"
            top={0}
            w="100%"
            zIndex={0}
          >
            {timeTicks.map(({ leftPct }) => (
              <Box
                borderColor="border"
                borderLeftWidth={1}
                h="100%"
                key={`gantt-body-grid-${leftPct}`}
                left={`calc(${leftPct}% - 0.25px)`}
                position="absolute"
                top={0}
                w={0}
              />
            ))}
          </Box>
          {virtualItems.map((vItem) => {
            const node = flatNodes[vItem.index];

            if (node === undefined) {
              return undefined;
            }

            const allSegments = rowSegments[vItem.index] ?? [];
            // Hide scheduled/queued bars that are too narrow to see. Re-derive adjacency
            // from the filtered list so the adjacent execution bar keeps rounded corners.
            const segments =
              bodyWidthPx > 0
                ? allSegments.filter((segment) =>
                    segment.state !== "scheduled" && segment.state !== "queued"
                      ? true
                      : segmentLayout(segment).widthPx >= MIN_SEGMENT_RENDER_PX,
                  )
                : allSegments;
            const taskId = node.id;
            const isSelected = selectedTaskId === taskId || selectedGroupId === taskId;
            const isHovered = hoveredTaskId === taskId;
            const gridSummary = summaryByTaskId.get(taskId);

            return (
              <Box
                borderBottomWidth={1}
                borderColor={node.isGroup ? "border.emphasized" : "border"}
                h={`${ROW_HEIGHT}px`}
                key={taskId}
                left={0}
                maxW="100%"
                position="absolute"
                top={0}
                transform={`translateY(${vItem.start}px)`}
                w="100%"
                zIndex={1}
              >
                <Box
                  bg={isSelected ? "brand.emphasized" : isHovered ? "brand.muted" : undefined}
                  h="100%"
                  maxW="100%"
                  onMouseEnter={() => setHoveredTaskId(taskId)}
                  onMouseLeave={() => setHoveredTaskId(undefined)}
                  overflow="hidden"
                  position="relative"
                  px="3px"
                  transition="background-color 0.2s"
                  w="100%"
                >
                  {segments.map((segment, segIndex) => {
                    const { leftPct, widthPct } = segmentLayout(segment);
                    const to = getGanttSegmentTo({
                      dagId,
                      item: segment,
                      maxTryByTaskId,
                      pathname,
                      runId,
                      searchParams: baseSearchParams,
                    });
                    const { state, tryNumber, x } = segment;
                    const tooltipInstance = toTooltipSummary(segment, node, gridSummary);
                    const barRadius = 4;

                    // Task groups don't have a try number
                    const touchesNext =
                      tryNumber === undefined ? false : segments[segIndex + 1]?.tryNumber === tryNumber;
                    const touchesPrev =
                      tryNumber === undefined ? false : segments[segIndex - 1]?.tryNumber === tryNumber;

                    return (
                      <TaskInstanceTooltip
                        key={`${taskId}-${tryNumber ?? -1}-${x[0]}`}
                        openDelay={500}
                        positioning={{
                          offset: { crossAxis: 0, mainAxis: 5 },
                          placement: "bottom",
                        }}
                        taskInstance={tooltipInstance}
                      >
                        <Box
                          as="span"
                          display="block"
                          h={`${TASK_BAR_HEIGHT_PX}px`}
                          left={`${leftPct}%`}
                          maxW={`${100 - leftPct}%`}
                          minW={`${MIN_BAR_WIDTH_PX}px`}
                          position="absolute"
                          top={`${(ROW_HEIGHT - TASK_BAR_HEIGHT_PX) / 2}px`}
                          w={`${widthPct}%`}
                          zIndex={segIndex + 1}
                        >
                          <Link
                            onClick={() => onSegmentClick?.()}
                            replace
                            style={{ display: "block", height: "100%", width: "100%" }}
                            to={to ?? ""}
                          >
                            <Badge
                              alignItems="center"
                              borderBottomLeftRadius={touchesPrev ? 0 : barRadius}
                              borderBottomRightRadius={touchesNext ? 0 : barRadius}
                              borderTopLeftRadius={touchesPrev ? 0 : barRadius}
                              borderTopRightRadius={touchesNext ? 0 : barRadius}
                              colorPalette={state ?? "none"}
                              display="flex"
                              h="100%"
                              justifyContent="center"
                              minH={0}
                              p={0}
                              variant="solid"
                              w="100%"
                            >
                              {touchesNext ? undefined : (
                                <StateIcon size={GANTT_STATE_ICON_SIZE_PX} state={state} />
                              )}
                            </Badge>
                          </Link>
                        </Box>
                      </TaskInstanceTooltip>
                    );
                  })}
                </Box>
              </Box>
            );
          })}
        </Box>
      </Box>
    </Box>
  );
};
