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

import React, { useCallback, useEffect, useRef, useState } from "react";
import { Alert, AlertIcon, Box, Divider, Text } from "@chakra-ui/react";

import { useGridData } from "src/api";
import Time from "src/components/Time";
import { getDuration } from "src/datetime_utils";

import Row from "./Row";

interface Props {
  runId: string | null;
  taskId: string | null;
  openGroupIds: string[];
  gridScrollRef: React.RefObject<HTMLDivElement>;
  ganttScrollRef: React.RefObject<HTMLDivElement>;
}

const Gantt = ({
  runId,
  taskId,
  openGroupIds,
  gridScrollRef,
  ganttScrollRef,
}: Props) => {
  const ganttRef = useRef<HTMLDivElement>(null);
  const [top, setTop] = useState(0);
  const [width, setWidth] = useState(500);
  const [height, setHeight] = useState("100%");
  const [startDate, setStartDate] = useState<string | null | undefined>();
  const [endDate, setEndDate] = useState<string | null | undefined>();

  const {
    data: { dagRuns, groups },
  } = useGridData();

  const calculateGanttDimensions = useCallback(() => {
    if (ganttRef?.current) {
      const tbody = gridScrollRef.current?.getElementsByTagName("tbody")[0];
      const tableTop =
        (tbody?.getBoundingClientRect().top || 0) +
        (gridScrollRef?.current?.scrollTop || 0);
      const ganttRect = ganttRef?.current?.getBoundingClientRect();
      const offsetTop = ganttRect?.top;
      setTop(tableTop && offsetTop ? tableTop - offsetTop : 0);
      if (ganttRect?.width) setWidth(ganttRect.width);
      const gridHeight = gridScrollRef.current?.getBoundingClientRect().height;
      if (gridHeight) setHeight(`${gridHeight - 155}px`);
    }
  }, [ganttRef, gridScrollRef]);

  // Calculate top, height and width when changing selections
  useEffect(() => {
    calculateGanttDimensions();
  }, [runId, taskId, openGroupIds, calculateGanttDimensions]);

  const onGridScroll = (e: Event) => {
    const { scrollTop } = e.currentTarget as HTMLDivElement;
    if (scrollTop && ganttScrollRef?.current) {
      ganttScrollRef.current.scrollTo(0, scrollTop);

      // Double check scroll position after 100ms
      setTimeout(() => {
        const gridScrollTop = gridScrollRef.current?.scrollTop;
        const ganttScrollTop = ganttScrollRef.current?.scrollTop;
        if (ganttScrollTop !== gridScrollTop && ganttScrollRef.current) {
          ganttScrollRef.current.scrollTo(0, gridScrollTop || 0);
        }
      }, 100);
    }
  };

  // Sync grid and gantt scroll
  useEffect(() => {
    const grid = gridScrollRef.current;
    grid?.addEventListener("scroll", onGridScroll);
    return () => {
      grid?.removeEventListener("scroll", onGridScroll);
    };
  });

  // Calculate top, height and width on resize
  useEffect(() => {
    const ganttChart = ganttRef.current;
    const ganttObserver = new ResizeObserver(calculateGanttDimensions);

    if (ganttChart) {
      ganttObserver.observe(ganttChart);
      return () => {
        ganttObserver.unobserve(ganttChart);
      };
    }
    return () => {};
  }, [ganttRef, calculateGanttDimensions]);

  const dagRun = dagRuns.find((dr) => dr.runId === runId);

  // Check if any task instance dates are outside the bounds of the dag run dates and update our min start and max end
  const setGanttDuration = useCallback(
    (
      queued: string | null | undefined,
      start: string | null | undefined,
      end: string | null | undefined
    ) => {
      if (
        queued &&
        (!startDate || Date.parse(queued) < Date.parse(startDate))
      ) {
        setStartDate(queued);
      } else if (
        start &&
        (!startDate || Date.parse(start) < Date.parse(startDate))
      ) {
        setStartDate(start);
      }

      if (end && (!endDate || Date.parse(end) > Date.parse(endDate))) {
        setEndDate(end);
      } else if (!end) {
        // @ts-ignore
        setEndDate(moment().add(1, "s").toString());
      }
    },
    [startDate, endDate, setStartDate, setEndDate]
  );

  // Reset state when the dagrun changes
  useEffect(() => {
    setStartDate(dagRun?.queuedAt || dagRun?.startDate);
    // @ts-ignore
    setEndDate(dagRun?.endDate ?? moment().add(1, "s").toString());
  }, [dagRun?.queuedAt, dagRun?.startDate, dagRun?.endDate]);

  const numBars = Math.round(width / 100);
  const runDuration = getDuration(startDate, endDate);
  const intervals = runDuration / numBars;

  return (
    <Box ref={ganttRef} position="relative" height="100%" overflow="hidden">
      {!runId && (
        <Alert status="warning" position="absolute" top={2}>
          <AlertIcon />
          Please select a dag run in order to see a gantt chart
        </Alert>
      )}
      <Box borderBottomWidth={1} pt={`${top}px`} pointerEvents="none">
        {Array.from(Array(numBars)).map((_, i) => (
          <Box
            position="absolute"
            left={`${(width / numBars) * i}px`}
            // eslint-disable-next-line react/no-array-index-key
            key={i}
          >
            <Text
              color="gray.400"
              fontSize="sm"
              transform="rotate(-30deg) translateX(28px)"
              mt={-6}
              mb={1}
              ml={-9}
            >
              <Time
                // @ts-ignore
                dateTime={moment(startDate)
                  .add(i * intervals, "milliseconds")
                  .format()}
                format="HH:mm:ss z"
              />
            </Text>
            <Divider orientation="vertical" height={height} />
          </Box>
        ))}
        <Box position="absolute" left={width - 2} key="end">
          <Divider orientation="vertical" height={height} />
        </Box>
      </Box>
      <Box
        maxHeight={height}
        height="100%"
        overflowY="scroll"
        ref={ganttScrollRef}
        overscrollBehavior="contain"
      >
        <div>
          {!!runId &&
            !!dagRun &&
            !!groups.children &&
            groups.children.map((c) => (
              <Row
                ganttWidth={width}
                openGroupIds={openGroupIds}
                task={c}
                ganttStartDate={startDate}
                ganttEndDate={endDate}
                setGanttDuration={setGanttDuration}
                key={`gantt-${c.id}`}
              />
            ))}
        </div>
      </Box>
    </Box>
  );
};

export default Gantt;
