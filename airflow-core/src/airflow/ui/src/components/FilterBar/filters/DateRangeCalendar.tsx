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
import { Button, Grid, HStack, Text, VStack, Box } from "@chakra-ui/react";
import dayjs, { type Dayjs } from "dayjs";
import { useMemo } from "react";
import { MdChevronLeft, MdChevronRight } from "react-icons/md";

import { useCalendarSelect } from "src/hooks/useCalendarSelect";

import type { DateRangeValue } from "../types";
import { isValidDateValue } from "../utils";

type DateRangeCalendarProps = {
  readonly currentMonth: Dayjs;
  readonly onDateClick: (date: Dayjs) => void;
  readonly onMonthChange: (month: Dayjs) => void;
  readonly value: DateRangeValue;
};

const getDayState = (day: Dayjs, currentMonth: Dayjs, dateRange: { endDate?: Dayjs; startDate?: Dayjs }) => {
  const isCurrentMonth = day.isSame(currentMonth, "month");
  const isEnd = Boolean(dateRange.endDate?.isSame(day, "day"));
  const isInRange = Boolean(
    dateRange.startDate &&
    dateRange.endDate &&
    day.isAfter(dateRange.startDate, "day") &&
    day.isBefore(dateRange.endDate, "day"),
  );
  const isStart = Boolean(dateRange.startDate?.isSame(day, "day"));
  const isToday = day.isSame(dayjs(), "day");

  return { isCurrentMonth, isEnd, isInRange, isStart, isToday };
};

export const DateRangeCalendar = ({
  currentMonth,
  onDateClick,
  onMonthChange,
  value,
}: DateRangeCalendarProps) => {
  const { days } = useCalendarSelect(currentMonth);

  const startDateValue = isValidDateValue(value.startDate) ? dayjs(value.startDate) : undefined;
  const endDateValue = isValidDateValue(value.endDate) ? dayjs(value.endDate) : undefined;

  const weekdayHeaders = useMemo(() => days.slice(0, 7).map((day) => day.format("dd")), [days]);

  const getDateStyles = useMemo(
    () => (day: Dayjs) => {
      const state = getDayState(day, currentMonth, { endDate: endDateValue, startDate: startDateValue });

      const getBgColor = () => {
        if (state.isStart) {
          return "brand.solid";
        }
        if (state.isEnd) {
          return "brand.muted";
        }
        if (state.isInRange) {
          return "brand.subtle";
        }

        return "transparent";
      };

      const getTextColor = () => {
        if (state.isStart || state.isEnd) {
          return "brand.contrast";
        }
        if (!state.isCurrentMonth) {
          return "fg.muted";
        }

        return "fg";
      };

      return {
        bgColor: getBgColor(),
        showTodayIndicator: state.isToday && !state.isStart && !state.isEnd,
        state,
        textColor: getTextColor(),
      };
    },
    [currentMonth, startDateValue, endDateValue],
  );

  return (
    <>
      {/* Month Navigation */}
      <HStack justify="space-between" w="full">
        <Button onClick={() => onMonthChange(currentMonth.subtract(1, "month"))} size="xs" variant="ghost">
          <MdChevronLeft />
        </Button>
        <Text fontSize="sm" fontWeight="medium">
          {currentMonth.format("MMM YYYY")}
        </Text>
        <Button onClick={() => onMonthChange(currentMonth.add(1, "month"))} size="xs" variant="ghost">
          <MdChevronRight />
        </Button>
      </HStack>

      {/* Calendar Grid */}
      <VStack gap={2} w="full">
        {/* Day Headers */}
        <Grid gap={1} gridTemplateColumns="repeat(7, 1fr)" w="full">
          {weekdayHeaders.map((dayName) => (
            <Text
              color="fg.muted"
              fontSize="2xs"
              fontWeight="medium"
              key={dayName}
              py={0.5}
              textAlign="center"
              w="28px"
            >
              {dayName}
            </Text>
          ))}
        </Grid>

        {/* Calendar Days */}
        <Grid gap={1} gridTemplateColumns="repeat(7, 1fr)" w="full">
          {days.map((dayItem) => {
            const styles = getDateStyles(dayItem);
            const isSelected = styles.state.isStart || styles.state.isEnd;

            return (
              <Button
                _hover={{
                  bg: isSelected ? styles.bgColor : "bg.muted",
                }}
                bg={styles.bgColor}
                border="1px solid transparent"
                color={styles.textColor}
                fontSize="xs"
                fontWeight="normal"
                h="28px"
                key={dayItem.format("YYYY-MM-DD")}
                minW="28px"
                onClick={() => onDateClick(dayItem)}
                p={0}
                position="relative"
                size="xs"
                variant="ghost"
              >
                {dayItem.date()}
                {Boolean(styles.showTodayIndicator) && (
                  <Box
                    bg="red.solid"
                    borderRadius="full"
                    bottom="1"
                    height="3px"
                    left="50%"
                    position="absolute"
                    transform="translateX(-50%)"
                    width="3px"
                  />
                )}
              </Button>
            );
          })}
        </Grid>
      </VStack>
    </>
  );
};
