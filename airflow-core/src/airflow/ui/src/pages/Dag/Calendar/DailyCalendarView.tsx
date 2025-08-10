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

/*
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
import dayjs from "dayjs";
import { useRef } from "react";

import type { CalendarTimeRangeResponse } from "openapi/requests/types.gen";

import { CalendarTooltip } from "./CalendarTooltip";
import { createTooltipContent, generateDailyCalendarData, getCalendarCellColor } from "./calendarUtils";

type Props = {
  readonly cellSize: number;
  readonly data: Array<CalendarTimeRangeResponse>;
  readonly selectedYear: number;
};

export const DailyCalendarView = ({ cellSize, data, selectedYear }: Props) => {
  const dailyData = generateDailyCalendarData(data, selectedYear);
  const debounceTimeoutRef = useRef<NodeJS.Timeout | undefined>(undefined);
  const activeTooltipRef = useRef<HTMLElement | undefined>(undefined);

  const handleMouseEnter = (event: React.MouseEvent<HTMLDivElement>) => {
    if (debounceTimeoutRef.current) {
      clearTimeout(debounceTimeoutRef.current);
    }

    const tooltipElement = event.currentTarget.querySelector("[data-tooltip]");

    if (tooltipElement) {
      activeTooltipRef.current = tooltipElement as HTMLElement;
      debounceTimeoutRef.current = setTimeout(() => {
        if (activeTooltipRef.current) {
          activeTooltipRef.current.style.opacity = "1";
          activeTooltipRef.current.style.visibility = "visible";
        }
      }, 200);
    }
  };

  const handleMouseLeave = () => {
    if (debounceTimeoutRef.current) {
      clearTimeout(debounceTimeoutRef.current);
      debounceTimeoutRef.current = undefined;
    }

    if (activeTooltipRef.current) {
      activeTooltipRef.current.style.opacity = "0";
      activeTooltipRef.current.style.visibility = "hidden";
      activeTooltipRef.current = undefined;
    }
  };

  return (
    <Box mb={4}>
      <Box display="flex" mb={2}>
        <Box width="50px" />
        <Box display="flex" gap={1}>
          {dailyData.map((week, index) => (
            <Box key={`month-${week[0]?.date ?? index}`} position="relative" width={`${cellSize}px`}>
              {Boolean(week[0] && dayjs(week[0].date).date() <= 7) && (
                <Text color="fg.muted" fontSize="xs" left="0" position="absolute" top="-20px">
                  {dayjs(week[0]?.date).format("MMM")}
                </Text>
              )}
            </Box>
          ))}
        </Box>
      </Box>
      <Box display="flex" gap={2}>
        <Box display="flex" flexDirection="column" gap={1}>
          {["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"].map((day) => (
            <Box
              alignItems="center"
              color="fg.muted"
              display="flex"
              fontSize="xs"
              height={`${cellSize}px`}
              justifyContent="flex-end"
              key={day}
              pr={2}
              width="40px"
            >
              {day}
            </Box>
          ))}
        </Box>
        <Box display="flex" gap={1}>
          {dailyData.map((week, weekIndex) => (
            <Box display="flex" flexDirection="column" gap={1} key={`week-${week[0]?.date ?? weekIndex}`}>
              {week.map((day) => {
                const dayDate = dayjs(day.date);
                const isInSelectedYear = dayDate.year() === selectedYear;

                if (!isInSelectedYear) {
                  return (
                    <Box bg="transparent" height={`${cellSize}px`} key={day.date} width={`${cellSize}px`} />
                  );
                }

                return (
                  <Box
                    key={day.date}
                    onMouseEnter={handleMouseEnter}
                    onMouseLeave={handleMouseLeave}
                    position="relative"
                  >
                    <Box
                      _hover={{ transform: "scale(1.1)" }}
                      bg={getCalendarCellColor(day.runs)}
                      borderRadius="2px"
                      cursor="pointer"
                      height={`${cellSize}px`}
                      width={`${cellSize}px`}
                    />
                    <CalendarTooltip cellSize={cellSize} content={createTooltipContent(day)} />
                  </Box>
                );
              })}
            </Box>
          ))}
        </Box>
      </Box>
    </Box>
  );
};
