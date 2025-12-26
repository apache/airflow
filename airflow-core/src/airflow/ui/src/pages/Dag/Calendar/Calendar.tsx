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
import { Box, HStack, Text, IconButton, Button, ButtonGroup } from "@chakra-ui/react";
import { keyframes } from "@emotion/react";
import dayjs from "dayjs";
import { useState, useMemo } from "react";
import { useTranslation } from "react-i18next";
import { FiChevronLeft, FiChevronRight } from "react-icons/fi";
import { useParams } from "react-router-dom";
import { useLocalStorage } from "usehooks-ts";

import { useCalendarServiceGetCalendar } from "openapi/queries";
import { ErrorAlert } from "src/components/ErrorAlert";

import { CalendarLegend } from "./CalendarLegend";
import { DailyCalendarView } from "./DailyCalendarView";
import { HourlyCalendarView } from "./HourlyCalendarView";
import { createCalendarScale } from "./calendarUtils";

const spin = keyframes`
  from { transform: rotate(0deg); }
  to { transform: rotate(360deg); }
`;

export const Calendar = () => {
  const { dagId = "" } = useParams();
  const { t: translate } = useTranslation("dag");
  const [selectedDate, setSelectedDate] = useState(dayjs());
  const [granularity, setGranularity] = useLocalStorage<"daily" | "hourly">("calendar-granularity", "hourly");
  const [viewMode, setViewMode] = useLocalStorage<"failed" | "total">("calendar-view-mode", "total");

  const currentDate = dayjs();

  const dateRange = useMemo(() => {
    if (granularity === "daily") {
      const yearStart = selectedDate.startOf("year");
      const yearEnd = selectedDate.endOf("year");

      return {
        logicalDateGte: yearStart.format("YYYY-MM-DD[T]HH:mm:ss[Z]"),
        logicalDateLte: yearEnd.format("YYYY-MM-DD[T]HH:mm:ss[Z]"),
      };
    } else {
      const monthStart = selectedDate.startOf("month");
      const monthEnd = selectedDate.endOf("month");

      return {
        logicalDateGte: monthStart.format("YYYY-MM-DD[T]HH:mm:ss[Z]"),
        logicalDateLte: monthEnd.format("YYYY-MM-DD[T]HH:mm:ss[Z]"),
      };
    }
  }, [granularity, selectedDate]);

  const { data, error, isLoading } = useCalendarServiceGetCalendar(
    {
      dagId,
      granularity,
      ...dateRange,
    },
    undefined,
    { enabled: Boolean(dagId) },
  );

  const scale = useMemo(
    () => createCalendarScale(data?.dag_runs ?? [], viewMode, granularity),
    [data?.dag_runs, viewMode, granularity],
  );

  if (!data && !isLoading) {
    return (
      <Box p={4}>
        <Text>{translate("calendar.noData")}</Text>
      </Box>
    );
  }

  return (
    <Box p={6}>
      <ErrorAlert error={error} />
      <HStack justify="space-between" mb={6}>
        <HStack gap={4} mb={4}>
          {granularity === "daily" ? (
            <HStack gap={2}>
              <IconButton
                aria-label={translate("calendar.navigation.previousYear")}
                onClick={() => setSelectedDate(selectedDate.subtract(1, "year"))}
                size="sm"
                variant="ghost"
              >
                <FiChevronLeft />
              </IconButton>
              <Text
                _hover={selectedDate.year() === currentDate.year() ? {} : { textDecoration: "underline" }}
                color={selectedDate.year() === currentDate.year() ? "fg.info" : "inherit"}
                cursor={selectedDate.year() === currentDate.year() ? "default" : "pointer"}
                fontSize="xl"
                fontWeight="bold"
                minWidth="120px"
                onClick={() => {
                  if (selectedDate.year() !== currentDate.year()) {
                    setSelectedDate(currentDate.startOf("year"));
                  }
                }}
                textAlign="center"
              >
                {selectedDate.year()}
              </Text>
              <IconButton
                aria-label={translate("calendar.navigation.nextYear")}
                onClick={() => setSelectedDate(selectedDate.add(1, "year"))}
                size="sm"
                variant="ghost"
              >
                <FiChevronRight />
              </IconButton>
            </HStack>
          ) : (
            <HStack gap={2}>
              <IconButton
                aria-label={translate("calendar.navigation.previousMonth")}
                onClick={() => setSelectedDate(selectedDate.subtract(1, "month"))}
                size="sm"
                variant="ghost"
              >
                <FiChevronLeft />
              </IconButton>
              <Text
                _hover={
                  selectedDate.isSame(currentDate, "month") && selectedDate.isSame(currentDate, "year")
                    ? {}
                    : { textDecoration: "underline" }
                }
                color={
                  selectedDate.isSame(currentDate, "month") && selectedDate.isSame(currentDate, "year")
                    ? "fg.info"
                    : "inherit"
                }
                cursor={
                  selectedDate.isSame(currentDate, "month") && selectedDate.isSame(currentDate, "year")
                    ? "default"
                    : "pointer"
                }
                fontSize="xl"
                fontWeight="bold"
                minWidth="120px"
                onClick={() => {
                  if (
                    !(selectedDate.isSame(currentDate, "month") && selectedDate.isSame(currentDate, "year"))
                  ) {
                    setSelectedDate(currentDate.startOf("month"));
                  }
                }}
                textAlign="center"
              >
                {selectedDate.format("MMM YYYY")}
              </Text>
              <IconButton
                aria-label={translate("calendar.navigation.nextMonth")}
                onClick={() => setSelectedDate(selectedDate.add(1, "month"))}
                size="sm"
                variant="ghost"
              >
                <FiChevronRight />
              </IconButton>
            </HStack>
          )}

          <ButtonGroup attached size="sm" variant="outline">
            <Button
              colorPalette="brand"
              onClick={() => setGranularity("daily")}
              variant={granularity === "daily" ? "solid" : "outline"}
            >
              {translate("calendar.daily")}
            </Button>
            <Button
              colorPalette="brand"
              onClick={() => setGranularity("hourly")}
              variant={granularity === "hourly" ? "solid" : "outline"}
            >
              {translate("calendar.hourly")}
            </Button>
          </ButtonGroup>

          <ButtonGroup attached size="sm" variant="outline">
            <Button
              colorPalette="brand"
              onClick={() => setViewMode("total")}
              variant={viewMode === "total" ? "solid" : "outline"}
            >
              {translate("calendar.totalRuns")}
            </Button>
            <Button
              colorPalette="brand"
              onClick={() => setViewMode("failed")}
              variant={viewMode === "failed" ? "solid" : "outline"}
            >
              {translate("overview.buttons.failedRun_other")}
            </Button>
          </ButtonGroup>
        </HStack>
      </HStack>

      <Box position="relative">
        {isLoading ? (
          <Box
            alignItems="center"
            backdropFilter="blur(2px)"
            bg="bg/80"
            borderRadius="md"
            bottom="0"
            display="flex"
            justifyContent="center"
            left="0"
            position="absolute"
            right="0"
            top="0"
            zIndex={10}
          >
            <Box textAlign="center">
              <Box
                animation={`${spin} 1s linear infinite`}
                border="3px solid"
                borderColor={{ _dark: "none.600", _light: "brand.100" }}
                borderRadius="50%"
                borderTopColor="brand.500"
                height="24px"
                width="24px"
              />
            </Box>
          </Box>
        ) : undefined}
        {granularity === "daily" ? (
          <>
            <DailyCalendarView
              data={data?.dag_runs ?? []}
              scale={scale}
              selectedYear={selectedDate.year()}
              viewMode={viewMode}
            />
            <CalendarLegend scale={scale} viewMode={viewMode} />
          </>
        ) : (
          <HStack align="start" gap={2}>
            <Box>
              <HourlyCalendarView
                data={data?.dag_runs ?? []}
                scale={scale}
                selectedMonth={selectedDate.month()}
                selectedYear={selectedDate.year()}
                viewMode={viewMode}
              />
            </Box>
            <Box display="flex" flex="1" justifyContent="center" pt={16}>
              <CalendarLegend scale={scale} vertical viewMode={viewMode} />
            </Box>
          </HStack>
        )}
      </Box>
    </Box>
  );
};
