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
import { Box, HStack, Text, VStack } from "@chakra-ui/react";
import { useTranslation } from "react-i18next";
import { useParams } from "react-router-dom";

import { useCalendarServiceGetCalendar } from "openapi/queries";
import { Tooltip } from "src/components/ui";

const getColor = (count: number) => {
  if (count === 0) {
    return "#ebedf0";
  }
  if (count <= 2) {
    return "#9be9a8";
  }
  if (count <= 5) {
    return "#40c463";
  }
  if (count <= 8) {
    return "#30a14e";
  }

  return "#216e39";
};

export const Calendar = () => {
  const { dagId = "" } = useParams();
  const { t: translate } = useTranslation("dag");

  const { data, error, isLoading } = useCalendarServiceGetCalendar(
    {
      dagId,
      granularity: "daily",
      logicalDateGte: `${new Date().getFullYear()}-01-01T00:00:00Z`,
      logicalDateLte: `${new Date().getFullYear()}-12-31T23:59:59Z`,
    },
    undefined,
    {
      enabled: Boolean(dagId),
    },
  );

  if (isLoading) {
    return (
      <Box p={4}>
        <Text>{translate("calendar.loading", "Loading...")}</Text>
      </Box>
    );
  }

  if (Boolean(error)) {
    return (
      <Box p={4}>
        <Text color="red.500">{translate("calendar.error", "Error loading calendar data")}</Text>
      </Box>
    );
  }

  if (!data) {
    return (
      <Box p={4}>
        <Text>{translate("calendar.noData", "No data available")}</Text>
      </Box>
    );
  }

  const generateCalendarGrid = () => {
    const currentYear = new Date().getFullYear();
    const startDate = new Date(currentYear, 0, 1);
    const endDate = new Date(currentYear, 11, 31);

    const weeks = [];
    const iterDate = new Date(startDate);

    const dayOfWeek = iterDate.getDay();

    iterDate.setDate(iterDate.getDate() - dayOfWeek);

    const currentIterDate = new Date(iterDate);
    const endTime = endDate.getTime();

    while (currentIterDate.getTime() <= endTime) {
      const week = [];

      for (let dayIndex = 0; dayIndex < 7; dayIndex += 1) {
        const [dateStr] = currentIterDate.toISOString().split("T");
        const dayData = data.dag_runs.find((run) => run.date.split("T")[0] === dateStr);

        week.push({
          count: dayData?.count ?? 0,
          date: dateStr,
          isCurrentYear: currentIterDate.getFullYear() === currentYear,
          state: dayData?.state ?? "none",
        });

        currentIterDate.setDate(currentIterDate.getDate() + 1);
      }
      weeks.push(week);
    }

    return weeks;
  };

  const calendarWeeks = generateCalendarGrid();

  const monthNames = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];
  const dayNames = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"];
  const legendCounts = [0, 1, 3, 6, 10];

  return (
    <Box p={6}>
      <VStack align="stretch" gap={6}>
        <Box>
          <Text fontSize="2xl" fontWeight="bold" mb={2}>
            {translate("calendar.dagId", { dagId })}
          </Text>
          <HStack gap={4}>
            <Text color="gray.600">
              {translate("calendar.totalEntries", "Total entries: {{count}}", {
                count: data.total_entries || 0,
              })}
            </Text>
            <Text color="gray.600">
              {translate("calendar.dagRuns", "DAG runs: {{count}}", { count: data.dag_runs.length })}
            </Text>
          </HStack>
        </Box>

        <Box>
          <Text fontSize="lg" fontWeight="semibold" mb={4}>
            {translate("calendar.activityTitle", "DAG Run Activity - {{year}}", {
              year: new Date().getFullYear(),
            })}
          </Text>

          <Box mb={2}>
            <HStack gap="0" justify="flex-start">
              {monthNames.map((month, monthIndex) => (
                <Box color="gray.500" fontSize="xs" key={month} textAlign="left" width="52px">
                  {monthIndex === 0 || monthIndex % 2 === 0 ? month : ""}
                </Box>
              ))}
            </HStack>
          </Box>

          <Box display="flex" gap="3px">
            <Box display="flex" flexDirection="column" gap="3px" mr={2}>
              {dayNames.map((day, dayIndex) => (
                <Box
                  alignItems="center"
                  color="gray.500"
                  display="flex"
                  fontSize="xs"
                  height="11px"
                  key={day}
                >
                  {dayIndex % 2 === 1 ? day : ""}
                </Box>
              ))}
            </Box>

            <Box display="flex" gap="3px">
              {calendarWeeks.map((week) => (
                <Box
                  display="flex"
                  flexDirection="column"
                  gap="3px"
                  key={`week-${week[0]?.date ?? "unknown"}`}
                >
                  {week.map((day) => (
                    <Tooltip
                      content={
                        day.isCurrentYear
                          ? translate("calendar.tooltip", "{{date}}: {{count}} run{{plural}} {{state}}", {
                              count: day.count,
                              date: day.date,
                              plural: day.count === 1 ? "" : "s",
                              state: day.state === "none" ? "" : `(${day.state})`,
                            })
                          : day.date
                      }
                      key={day.date}
                    >
                      <Box
                        _hover={{
                          ring: 2,
                          ringColor: "blue.400",
                          transform: "scale(1.2)",
                        }}
                        bg={day.isCurrentYear ? getColor(day.count) : "#f1f5f9"}
                        borderRadius="2px"
                        cursor="pointer"
                        height="11px"
                        opacity={day.isCurrentYear ? 1 : 0.3}
                        transition="all 0.15s"
                        width="11px"
                      />
                    </Tooltip>
                  ))}
                </Box>
              ))}
            </Box>
          </Box>

          <Box mt={4}>
            <HStack color="gray.600" fontSize="xs" gap={2}>
              <Text>{translate("calendar.less", "Less")}</Text>
              <HStack gap="3px">
                {legendCounts.map((count) => (
                  <Box bg={getColor(count)} borderRadius="2px" height="11px" key={count} width="11px" />
                ))}
              </HStack>
              <Text>{translate("calendar.more", "More")}</Text>
            </HStack>
          </Box>
        </Box>

        <Box bg="gray.50" borderRadius="md" p={4}>
          <Text fontWeight="bold" mb={3}>
            {translate("calendar.summary", "Recent Activity Summary:")}
          </Text>
          <VStack align="stretch" gap={1}>
            {data.dag_runs.slice(0, 10).map((run) => (
              <HStack fontSize="sm" justify="space-between" key={`${run.date}-${run.state}-${run.count}`}>
                <Text>{run.date}</Text>
                <HStack gap={2}>
                  <Text fontWeight="medium">{translate("calendar.runCount", { count: run.count })}</Text>
                  <Box
                    bg={
                      run.state === "success"
                        ? "green.100"
                        : run.state === "failed"
                          ? "red.100"
                          : run.state === "running"
                            ? "blue.100"
                            : run.state === "queued"
                              ? "orange.100"
                              : "gray.100"
                    }
                    borderRadius="sm"
                    color={
                      run.state === "success"
                        ? "green.800"
                        : run.state === "failed"
                          ? "red.800"
                          : run.state === "running"
                            ? "blue.800"
                            : run.state === "queued"
                              ? "orange.800"
                              : "gray.800"
                    }
                    fontSize="xs"
                    fontWeight="medium"
                    px={2}
                    py={1}
                  >
                    {translate(`calendar.state.${run.state}`, run.state)}
                  </Box>
                </HStack>
              </HStack>
            ))}
          </VStack>
        </Box>
      </VStack>
    </Box>
  );
};
