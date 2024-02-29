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
import { Spinner } from "@chakra-ui/react";

import ReactECharts, { ReactEChartsProps } from "src/components/ReactECharts";
import { useCalendarData } from "src/api";

const Calendar = () => {
  const { data: calendarData, isLoading } = useCalendarData();

  if (isLoading) return <Spinner />;
  if (!calendarData) return null;

  const { dagStates } = calendarData;

  const startDate = dagStates[0].date;
  const endDate = dagStates[dagStates.length - 1].date;
  // @ts-ignore
  const startYear = moment(startDate).year();
  // @ts-ignore
  const endYear = moment(endDate).year();

  const calendarOption: EChartsOption["calendar"] = [];
  const seriesOption: EChartsOption["series"] = [];

  const flatDates: Record<string, any> = {};
  dagStates
    .filter((ds) => ds.state !== "planned")
    .forEach((ds) => {
      flatDates[ds.date] = {
        ...flatDates[ds.date],
        [ds.state]: ds.count,
      };
    });
  const proportions = Object.keys(flatDates).map((key) => {
    const date = key;
    const states = flatDates[key];
    const total =
      (states["failed"] || 0) +
      (states["success"] || 0) +
      (states["running"] || 0);
    const percent =
      ((states["success"] || 0) + (states["running"] || 0)) / total;
    return [date, (percent * 100).toFixed()];
  });
  // const plannedDates =

  if (startYear !== endYear) {
    for (let y = startYear; y <= endYear; y += 1) {
      const index = y - startYear;
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
        data: dagStates
          .filter(
            (ds) => ds.date.startsWith(y.toString()) && ds.state !== "planned"
          )
          .map((ds) => [ds.date, ds.count]),
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
        console.log(p);
        // TODO: get full data into tooltip to render correctly
        return `${p.data[0]} | Planned ${p.data[1]}`;
      },
    },
    visualMap: [
      {
        seriesIndex: scatterIndexes,
        inRange: {
          color: "gray",
          opacity: 0.6,
        },
      },
      {
        min: 0,
        max: 100,
        text: ["Success", "Failed"],
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
    ],
    calendar: calendarOption,
    series: seriesOption,
  };

  return <ReactECharts option={option} />;
};

export default Calendar;
