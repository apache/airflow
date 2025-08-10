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
import { Box, HStack, Text, IconButton, Button } from "@chakra-ui/react";
import { keyframes } from "@emotion/react";
import dayjs from "dayjs";
import { useState } from "react";
import { useTranslation } from "react-i18next";
import { FiMinus, FiPlus, FiChevronLeft, FiChevronRight } from "react-icons/fi";
import { useParams } from "react-router-dom";
import { useLocalStorage } from "usehooks-ts";

import { useCalendarServiceGetCalendar } from "openapi/queries";
import { ErrorAlert } from "src/components/ErrorAlert";

import { CalendarLegend } from "./CalendarLegend";
import { DailyCalendarView } from "./DailyCalendarView";
import { HourlyCalendarView } from "./HourlyCalendarView";

const spin = keyframes`
  from { transform: rotate(0deg); }
  to { transform: rotate(360deg); }
`;

export const Calendar = () => {
  const { dagId = "" } = useParams();
  const { t: translate } = useTranslation("dag");
  const [cellSize, setCellSize] = useLocalStorage("calendar-cell-size", 18);
  const [selectedYear, setSelectedYear] = useState(dayjs().year());
  const [selectedMonth, setSelectedMonth] = useState(dayjs().month());
  const [granularity, setGranularity] = useLocalStorage<"daily" | "hourly">("calendar-granularity", "daily");

  const currentYear = dayjs().year();
  const currentMonth = dayjs().month();

  const getDateRange = () => {
    if (granularity === "daily") {
      return {
        logicalDateGte: `${selectedYear}-01-01T00:00:00Z`,
        logicalDateLte: `${selectedYear}-12-31T23:59:59Z`,
      };
    } else {
      const monthStart = dayjs().year(selectedYear).month(selectedMonth).startOf("month");
      const monthEnd = dayjs().year(selectedYear).month(selectedMonth).endOf("month");

      return {
        logicalDateGte: monthStart.format("YYYY-MM-DD[T]HH:mm:ss[Z]"),
        logicalDateLte: monthEnd.format("YYYY-MM-DD[T]HH:mm:ss[Z]"),
      };
    }
  };

  const { data, error, isLoading } = useCalendarServiceGetCalendar(
    {
      dagId,
      granularity,
      ...getDateRange(),
    },
    undefined,
    { enabled: Boolean(dagId) },
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
        <HStack gap={4}>
          {granularity === "daily" ? (
            <HStack gap={2}>
              <IconButton
                aria-label="Previous year"
                onClick={() => setSelectedYear(selectedYear - 1)}
                size="sm"
                variant="ghost"
              >
                <FiChevronLeft />
              </IconButton>
              <Text
                _hover={{ textDecoration: "underline" }}
                color={selectedYear === currentYear ? "blue.500" : "inherit"}
                cursor="pointer"
                fontSize="xl"
                fontWeight="bold"
                minWidth="120px"
                onClick={() => setSelectedYear(currentYear)}
                textAlign="center"
              >
                {selectedYear}
              </Text>
              <IconButton
                aria-label="Next year"
                onClick={() => setSelectedYear(selectedYear + 1)}
                size="sm"
                variant="ghost"
              >
                <FiChevronRight />
              </IconButton>
            </HStack>
          ) : (
            <HStack gap={2}>
              <IconButton
                aria-label="Previous month"
                onClick={() => {
                  if (selectedMonth === 0) {
                    setSelectedMonth(11);
                    setSelectedYear(selectedYear - 1);
                  } else {
                    setSelectedMonth(selectedMonth - 1);
                  }
                }}
                size="sm"
                variant="ghost"
              >
                <FiChevronLeft />
              </IconButton>
              <Text
                _hover={{ textDecoration: "underline" }}
                color={
                  selectedYear === currentYear && selectedMonth === currentMonth ? "blue.500" : "inherit"
                }
                cursor="pointer"
                fontSize="xl"
                fontWeight="bold"
                minWidth="120px"
                onClick={() => {
                  setSelectedYear(currentYear);
                  setSelectedMonth(currentMonth);
                }}
                textAlign="center"
              >
                {dayjs().year(selectedYear).month(selectedMonth).format("MMM YYYY")}
              </Text>
              <IconButton
                aria-label="Next month"
                onClick={() => {
                  if (selectedMonth === 11) {
                    setSelectedMonth(0);
                    setSelectedYear(selectedYear + 1);
                  } else {
                    setSelectedMonth(selectedMonth + 1);
                  }
                }}
                size="sm"
                variant="ghost"
              >
                <FiChevronRight />
              </IconButton>
            </HStack>
          )}

          <HStack gap={0}>
            <Button
              colorScheme="blue"
              onClick={() => setGranularity("daily")}
              size="sm"
              variant={granularity === "daily" ? "solid" : "outline"}
            >
              {translate("calendar.daily")}
            </Button>
            <Button
              colorScheme="blue"
              onClick={() => setGranularity("hourly")}
              size="sm"
              variant={granularity === "hourly" ? "solid" : "outline"}
            >
              {translate("calendar.hourly")}
            </Button>
          </HStack>
        </HStack>

        <HStack gap={2}>
          <Text color="gray.600" fontSize="sm">
            {translate("calendar.cellSize")}:
          </Text>
          <IconButton
            aria-label={translate("calendar.decreaseSize")}
            disabled={cellSize <= 8}
            onClick={() => setCellSize(Math.max(8, cellSize - 1))}
            size="sm"
            variant="ghost"
          >
            <FiMinus />
          </IconButton>
          <Text fontSize="sm" minWidth="40px" textAlign="center">
            {cellSize}
            {translate("calendar.px")}
          </Text>
          <IconButton
            aria-label={translate("calendar.increaseSize")}
            disabled={cellSize >= 20}
            onClick={() => setCellSize(Math.min(20, cellSize + 1))}
            size="sm"
            variant="ghost"
          >
            <FiPlus />
          </IconButton>
        </HStack>
      </HStack>

      <Box position="relative">
        {isLoading ? (
          <Box
            alignItems="center"
            backdropFilter="blur(2px)"
            bg="rgba(255, 255, 255, 0.8)"
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
                borderColor="blue.100"
                borderRadius="50%"
                borderTopColor="blue.500"
                height="24px"
                width="24px"
              />
            </Box>
          </Box>
        ) : undefined}
        {granularity === "daily" ? (
          <>
            <DailyCalendarView cellSize={cellSize} data={data?.dag_runs ?? []} selectedYear={selectedYear} />
            <CalendarLegend />
          </>
        ) : (
          <HStack align="start" gap={2}>
            <Box>
              <HourlyCalendarView
                cellSize={cellSize}
                data={data?.dag_runs ?? []}
                selectedMonth={selectedMonth}
                selectedYear={selectedYear}
              />
            </Box>
            <Box display="flex" flex="1" justifyContent="center" pt={16}>
              <CalendarLegend vertical />
            </Box>
          </HStack>
        )}
      </Box>
    </Box>
  );
};
