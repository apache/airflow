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
import { useTranslation } from "react-i18next";

import type { CalendarTimeRangeResponse } from "openapi/requests/types.gen";

import { CalendarCell } from "./CalendarCell";
import { generateHourlyCalendarData } from "./calendarUtils";
import type { CalendarScale } from "./types";

dayjs.extend(isSameOrBefore);

type Props = {
  readonly data: Array<CalendarTimeRangeResponse>;
  readonly scale: CalendarScale;
  readonly selectedMonth: number;
  readonly selectedYear: number;
};

export const HourlyCalendarView = ({ data, scale, selectedMonth, selectedYear }: Props) => {
  const { t: translate } = useTranslation("dag");
  const hourlyData = generateHourlyCalendarData(data, selectedYear, selectedMonth);

  return (
    <Box mb={4}>
      <Box mb={4}>
        <Box display="flex" mb={2}>
          <Box width="40px" />
          <Box display="flex" gap={0.5}>
            {hourlyData.days.map((day, index) => {
              const isFirstOfWeek = index % 7 === 0;
              const weekNumber = Math.floor(index / 7) + 1;

              return (
                <Box
                  key={day.day}
                  marginRight={index % 7 === 6 ? "8px" : "0"}
                  position="relative"
                  width="14px"
                >
                  {Boolean(isFirstOfWeek) && (
                    <Text
                      color="fg.muted"
                      fontSize="xs"
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
          <Box display="flex" gap={0.5}>
            {hourlyData.days.map((day, index) => {
              const dayOfWeek = dayjs(day.day).day();
              const isWeekend = dayOfWeek === 0 || dayOfWeek === 6;
              const dateFontSize = "2xs";
              const dayNameFontSize = "2xs";
              const dayName = dayjs(day.day).format("dd").charAt(0);

              return (
                <Box key={day.day} marginRight={index % 7 === 6 ? "8px" : "0"} width="14px">
                  <Text
                    color={isWeekend ? "red.400" : "gray.600"}
                    fontSize={dateFontSize}
                    fontWeight={isWeekend ? "bold" : "normal"}
                    lineHeight="1"
                    textAlign="center"
                  >
                    {dayjs(day.day).format("D")}
                  </Text>
                  <Text
                    color={isWeekend ? "red.400" : "gray.500"}
                    fontSize={dayNameFontSize}
                    fontWeight={isWeekend ? "bold" : "normal"}
                    lineHeight="1"
                    mt="1px"
                    textAlign="center"
                  >
                    {dayName}
                  </Text>
                </Box>
              );
            })}
          </Box>
        </Box>
      </Box>

      <Box display="flex" gap={2}>
        <Box display="flex" flexDirection="column" gap={0.5}>
          {Array.from({ length: 24 }, (_, hour) => (
            <Box
              alignItems="center"
              color="gray.500"
              display="flex"
              fontSize="xs"
              height="14px"
              justifyContent="flex-end"
              key={hour}
              pr={2}
              width="30px"
            >
              {hour % 4 === 0 && hour.toString().padStart(2, "0")}
            </Box>
          ))}
        </Box>
        <Box display="flex" flexDirection="column" gap={0.5}>
          {Array.from({ length: 24 }, (_, hour) => (
            <Box display="flex" gap={0.5} key={hour}>
              {hourlyData.days.map((day, index) => {
                const hourData = day.hours.find((hourItem) => hourItem.hour === hour);

                if (!hourData) {
                  const emptyCounts = { failed: 0, planned: 0, queued: 0, running: 0, success: 0, total: 0 };
                  const emptyCellData = {
                    counts: emptyCounts,
                    date: `${day.day}T${hour.toString().padStart(2, "0")}:00:00`,
                    runs: [],
                  };

                  return (
                    <CalendarCell
                      backgroundColor={scale.getColor(emptyCounts)}
                      cellData={emptyCellData}
                      index={index}
                      key={`${day.day}-${hour}`}
                    />
                  );
                }

                return (
                  <CalendarCell
                    backgroundColor={scale.getColor(hourData.counts)}
                    cellData={hourData}
                    index={index}
                    key={`${day.day}-${hour}`}
                  />
                );
              })}
            </Box>
          ))}
        </Box>
      </Box>
    </Box>
  );
};
