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
import { useTranslation } from "react-i18next";

import type { CalendarTimeRangeResponse } from "openapi/requests/types.gen";

import { CalendarCell } from "./CalendarCell";
import { generateDailyCalendarData } from "./calendarUtils";
import type { CalendarScale } from "./types";

type Props = {
  readonly data: Array<CalendarTimeRangeResponse>;
  readonly scale: CalendarScale;
  readonly selectedYear: number;
};

export const DailyCalendarView = ({ data, scale, selectedYear }: Props) => {
  const { t: translate } = useTranslation("dag");
  const dailyData = generateDailyCalendarData(data, selectedYear);

  const weekdays = [
    translate("calendar.weekdays.sunday"),
    translate("calendar.weekdays.monday"),
    translate("calendar.weekdays.tuesday"),
    translate("calendar.weekdays.wednesday"),
    translate("calendar.weekdays.thursday"),
    translate("calendar.weekdays.friday"),
    translate("calendar.weekdays.saturday"),
  ];

  return (
    <Box mb={4}>
      <Box display="flex" mb={2}>
        <Box width="30px" />
        <Box display="flex" gap={0.5}>
          {dailyData.map((week, index) => (
            <Box key={`month-${week[0]?.date ?? index}`} position="relative" width="14px">
              {Boolean(week[0] && dayjs(week[0].date).date() <= 7) && (
                <Text color="fg.muted" fontSize="2xs" left="0" position="absolute" top="-20px">
                  {dayjs(week[0]?.date).format("MMM")}
                </Text>
              )}
            </Box>
          ))}
        </Box>
      </Box>
      <Box display="flex" gap={2}>
        <Box display="flex" flexDirection="column" gap={0.5}>
          {weekdays.map((day) => (
            <Box
              alignItems="center"
              color="fg.muted"
              display="flex"
              fontSize="2xs"
              height="14px"
              justifyContent="flex-end"
              key={day}
              pr={2}
              width="20px"
            >
              {day}
            </Box>
          ))}
        </Box>
        <Box display="flex" gap={0.5}>
          {dailyData.map((week, weekIndex) => (
            <Box display="flex" flexDirection="column" gap={0.5} key={`week-${week[0]?.date ?? weekIndex}`}>
              {week.map((day) => {
                const dayDate = dayjs(day.date);
                const isInSelectedYear = dayDate.year() === selectedYear;

                if (!isInSelectedYear) {
                  const emptyCellData = {
                    counts: { failed: 0, planned: 0, queued: 0, running: 0, success: 0, total: 0 },
                    date: day.date,
                    runs: [],
                  };

                  return (
                    <CalendarCell backgroundColor="transparent" cellData={emptyCellData} key={day.date} />
                  );
                }

                return (
                  <CalendarCell backgroundColor={scale.getColor(day.counts)} cellData={day} key={day.date} />
                );
              })}
            </Box>
          ))}
        </Box>
      </Box>
    </Box>
  );
};
