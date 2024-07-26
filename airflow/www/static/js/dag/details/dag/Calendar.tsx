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

/* global moment */

import React from "react";
import type { EChartsOption } from "echarts";
import { Box, Spinner, Flex, Text } from "@chakra-ui/react";

import ReactECharts from "src/components/ReactECharts";
import { useCalendarData } from "src/api";
import useFilters from "src/dag/useFilters";
import InfoTooltip from "src/components/InfoTooltip";

const Calendar = () => {
  const { onBaseDateChange } = useFilters();
  const { data: calendarData, isLoading } = useCalendarData();

  if (isLoading) return <Spinner />;
  if (!calendarData) return null;

  const { dagStates } = calendarData;

  if (dagStates.length < 1) {
    return <Text>Calendar view requires at least one DAG Run.</Text>;
  }

  const startDate = dagStates[0].date;
  const endDate = dagStates[dagStates.length - 1].date;
  // @ts-ignore
  const startYear = moment(startDate).year();
  // @ts-ignore
  const endYear = moment(endDate).year();

  const calendarOption: EChartsOption["calendar"] = [];
  const seriesOption: EChartsOption["series"] = [];

  const flatDates: Record<string, any> = {};
  const plannedDates: Record<string, any> = {};
  dagStates.forEach((ds) => {
    if (ds.state !== "planned") {
      flatDates[ds.date] = {
        ...flatDates[ds.date],
        [ds.state]: ds.count,
      };
    } else {
      plannedDates[ds.date] = {
        [ds.state]: ds.count,
      };
    }
  });

  const proportions = Object.keys(flatDates).map((key) => {
    const date = key;
    const states = flatDates[key];
    const total =
      (states.failed || 0) + (states.success || 0) + (states.running || 0);
    const percent = ((states.success || 0) + (states.running || 0)) / total;
    return [date, Math.round(percent * 100)];
  });

  // We need to split the data into multiple years of calendars
  if (startYear !== endYear) {
    for (let y = endYear; y >= startYear; y -= 1) {
      const index = endYear - y;
      const yearStartDate = y === startYear ? startDate : `${y}-01-01`;
      const yearEndDate = `${y}-12-31`;
      calendarOption.push({
        left: 100,
        top: index * 150 + 20,
        range: [yearStartDate, yearEndDate],
        cellSize: 15,
      });
      seriesOption.push({
        calendarIndex: index,
        type: "heatmap",
        coordinateSystem: "calendar",
        data: proportions.filter(
          (p) => typeof p[0] === "string" && p[0].startsWith(y.toString())
        ),
      });
      seriesOption.push({
        calendarIndex: index,
        type: "scatter",
        coordinateSystem: "calendar",
        symbolSize: 4,
        data: dagStates
          .filter(
            (ds) => ds.date.startsWith(y.toString()) && ds.state === "planned"
          )
          .map((ds) => [ds.date, ds.count]),
      });
    }
  } else {
    calendarOption.push({
      top: 20,
      left: 100,
      range: [startDate, `${endYear}-12-31`],
      cellSize: 15,
    });
    seriesOption.push({
      type: "heatmap",
      coordinateSystem: "calendar",
      data: proportions,
    });
    seriesOption.push({
      type: "scatter",
      coordinateSystem: "calendar",
      symbolSize: () => 4,
      data: dagStates
        .filter((ds) => ds.state === "planned")
        .map((ds) => [ds.date, ds.count]),
    });
  }

  const scatterIndexes: number[] = [];
  const heatmapIndexes: number[] = [];

  seriesOption.forEach((s, i) => {
    if (s.type === "heatmap") heatmapIndexes.push(i);
    else if (s.type === "scatter") scatterIndexes.push(i);
  });

  const option: EChartsOption = {
    tooltip: {
      formatter: (p: any) => {
        const date = p.data[0];
        const states = flatDates[date];
        const plannedCount =
          p.componentSubType === "scatter"
            ? p.data[1]
            : plannedDates[date]?.planned || 0;
        // @ts-ignore
        const formattedDate = moment(date).format("ddd YYYY-MM-DD");

        return `
          <strong>${formattedDate}</strong> <br>
          ${plannedCount ? `Planned ${plannedCount} <br>` : ""}
          ${states?.failed ? `Failed ${states.failed} <br>` : ""}
          ${states?.running ? `Running ${states.running} <br>` : ""}
          ${states?.success ? `Success ${states.success} <br>` : ""}
        `;
      },
    },
    visualMap: [
      {
        min: 0,
        max: 100,
        text: ["% Success", "Failed"],
        calculable: true,
        orient: "vertical",
        left: "0",
        top: "0",
        seriesIndex: heatmapIndexes,
        inRange: {
          color: [
            stateColors.failed,
            stateColors.up_for_retry,
            stateColors.success,
          ],
        },
      },
      {
        seriesIndex: scatterIndexes,
        inRange: {
          color: "gray",
          opacity: 0.6,
        },
        show: false,
      },
    ],
    calendar: calendarOption,
    series: seriesOption,
  };

  const events = {
    click(p: any) {
      onBaseDateChange(p.data[0]);
    },
  };

  return (
    <Box height={`${calendarOption.length * 165}px`} width="900px">
      <Flex>
        <InfoTooltip
          label="Only showing the next year of planned DAG runs or the next 2000 runs,
          whichever comes first."
          size={16}
        />
      </Flex>
      <ReactECharts option={option} events={events} />
    </Box>
  );
};

export default Calendar;
