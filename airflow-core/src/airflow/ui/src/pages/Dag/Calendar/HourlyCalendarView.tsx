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
import isSameOrBefore from "dayjs/plugin/isSameOrBefore";
import { useRef } from "react";
import { useTranslation } from "react-i18next";

import type { CalendarTimeRangeResponse } from "openapi/requests/types.gen";

import { CalendarTooltip } from "./CalendarTooltip";
import { createTooltipContent, generateHourlyCalendarData, getCalendarCellColor } from "./calendarUtils";

dayjs.extend(isSameOrBefore);

type Props = {
  readonly cellSize: number;
  readonly data: Array<CalendarTimeRangeResponse>;
  readonly selectedMonth: number;
  readonly selectedYear: number;
};

export const HourlyCalendarView = ({ cellSize, data, selectedMonth, selectedYear }: Props) => {
  const { t: translate } = useTranslation("dag");
  const hourlyData = generateHourlyCalendarData(data, selectedYear, selectedMonth);
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
      <Box mb={4}>
        <Box display="flex" mb={2}>
          <Box width="40px" />
          <Box display="flex" gap={1}>
            {hourlyData.days.map((day, index) => {
              const isFirstOfWeek = index % 7 === 0;
              const weekNumber = Math.floor(index / 7) + 1;

              return (
                <Box
                  key={day.day}
                  marginRight={index % 7 === 6 ? "8px" : "0"}
                  position="relative"
                  width={`${cellSize}px`}
                >
                  {Boolean(isFirstOfWeek) && (
                    <Text
                      color="blue.500"
                      fontSize="sm"
                      fontWeight="bold"
                      left="0"
                      position="absolute"
                      textAlign="left"
                      top="-25px"
                      whiteSpace="nowrap"
                    >
                      {translate("calendar.week", { weekNumber })}
                    </Text>
                  )}
                </Box>
              );
            })}
          </Box>
        </Box>
        <Box display="flex" mb={1}>
          <Box width="40px" />
          <Box display="flex" gap={1}>
            {hourlyData.days.map((day, index) => {
              const dayOfWeek = dayjs(day.day).day();
              const isWeekend = dayOfWeek === 0 || dayOfWeek === 6;

              return (
                <Box key={day.day} marginRight={index % 7 === 6 ? "8px" : "0"} width={`${cellSize}px`}>
                  <Text
                    color={isWeekend ? "red.400" : "gray.600"}
                    fontSize="xs"
                    fontWeight={isWeekend ? "bold" : "normal"}
                    textAlign="center"
                  >
                    {dayjs(day.day).format("D")}
                  </Text>
                  <Text
                    color={isWeekend ? "red.400" : "gray.500"}
                    fontSize="xs"
                    fontWeight={isWeekend ? "bold" : "normal"}
                    textAlign="center"
                  >
                    {dayjs(day.day).format("dd")}
                  </Text>
                </Box>
              );
            })}
          </Box>
        </Box>
      </Box>

      <Box display="flex" gap={2}>
        <Box display="flex" flexDirection="column" gap={1}>
          {Array.from({ length: 24 }, (_, hour) => (
            <Box
              alignItems="center"
              color="gray.500"
              display="flex"
              fontSize="xs"
              height={`${cellSize}px`}
              justifyContent="flex-end"
              key={hour}
              pr={2}
              width="30px"
            >
              {hour % 4 === 0 && hour.toString().padStart(2, "0")}
            </Box>
          ))}
        </Box>
        <Box display="flex" flexDirection="column" gap={1}>
          {Array.from({ length: 24 }, (_, hour) => (
            <Box display="flex" gap={1} key={hour}>
              {hourlyData.days.map((day, index) => {
                const hourData = day.hours.find((hourItem) => hourItem.hour === hour);

                if (!hourData) {
                  const noRunsTooltip = `${dayjs(day.day).format("MMM DD")}, ${hour.toString().padStart(2, "0")}:00 - No runs`;

                  return (
                    <Box
                      key={`${day.day}-${hour}`}
                      onMouseEnter={handleMouseEnter}
                      onMouseLeave={handleMouseLeave}
                      position="relative"
                    >
                      <Box
                        bg={getCalendarCellColor([])}
                        borderRadius="2px"
                        cursor="pointer"
                        height={`${cellSize}px`}
                        marginRight={index % 7 === 6 ? "8px" : "0"}
                        width={`${cellSize}px`}
                      />
                      <CalendarTooltip cellSize={cellSize} content={noRunsTooltip} />
                    </Box>
                  );
                }

                const tooltipContent =
                  hourData.counts.total > 0
                    ? `${dayjs(day.day).format("MMM DD")}, ${hour.toString().padStart(2, "0")}:00 - ${createTooltipContent(hourData).split(": ")[1]}`
                    : `${dayjs(day.day).format("MMM DD")}, ${hour.toString().padStart(2, "0")}:00 - No runs`;

                return (
                  <Box
                    key={`${day.day}-${hour}`}
                    onMouseEnter={handleMouseEnter}
                    onMouseLeave={handleMouseLeave}
                    position="relative"
                  >
                    <Box
                      bg={getCalendarCellColor(hourData.runs)}
                      borderRadius="2px"
                      cursor="pointer"
                      height={`${cellSize}px`}
                      marginRight={index % 7 === 6 ? "8px" : "0"}
                      width={`${cellSize}px`}
                    />
                    <CalendarTooltip cellSize={cellSize} content={tooltipContent} />
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
