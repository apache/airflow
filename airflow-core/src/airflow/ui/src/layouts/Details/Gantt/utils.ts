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
import type { ChartEvent, ActiveElement, TooltipItem } from "chart.js";
import type { TFunction } from "i18next";
import type { NavigateFunction, Location } from "react-router-dom";

import type { GridRunsResponse, TaskInstanceState } from "openapi/requests";
import { getDuration } from "src/utils";
import { formatDate } from "src/utils/datetimeUtils";
import { buildTaskInstanceUrl } from "src/utils/links";

export type GanttDataItem = {
  isGroup?: boolean | null;
  isMapped?: boolean | null;
  state?: TaskInstanceState | null;
  taskId: string;
  x: Array<string>;
  y: string;
};

type HandleBarClickOptions = {
  dagId: string;
  data: Array<GanttDataItem>;
  location: Location;
  navigate: NavigateFunction;
  runId: string;
};

type ChartOptionsParams = {
  data: Array<GanttDataItem>;
  gridColor?: string;
  handleBarClick: (event: ChartEvent, elements: Array<ActiveElement>) => void;
  selectedId?: string;
  selectedItemColor?: string;
  selectedRun?: GridRunsResponse;
  selectedTimezone: string;
  translate: TFunction;
};

export const createHandleBarClick =
  ({ dagId, data, location, navigate, runId }: HandleBarClickOptions) =>
  (_: ChartEvent, elements: Array<ActiveElement>) => {
    if (elements.length > 0 && elements[0] && Boolean(runId)) {
      const clickedData = data[elements[0].index];

      if (clickedData) {
        const { isGroup, isMapped, taskId } = clickedData;

        const taskUrl = buildTaskInstanceUrl({
          currentPathname: location.pathname,
          dagId,
          isGroup: Boolean(isGroup),
          isMapped: Boolean(isMapped),
          runId,
          taskId,
        });

        navigate(
          {
            pathname: taskUrl,
            search: location.search,
          },
          { replace: true },
        );
      }
    }
  };

export const createChartOptions = ({
  data,
  gridColor,
  handleBarClick,
  selectedId,
  selectedItemColor,
  selectedRun,
  selectedTimezone,
  translate,
}: ChartOptionsParams) => ({
  animation: {
    duration: 150,
    easing: "linear" as const,
  },
  indexAxis: "y" as const,
  maintainAspectRatio: false,
  onClick: handleBarClick,
  onHover: (event: ChartEvent, elements: Array<ActiveElement>) => {
    const target = event.native?.target as HTMLElement | undefined;

    if (target) {
      target.style.cursor = elements.length > 0 ? "pointer" : "default";
    }
  },
  plugins: {
    annotation: {
      annotations:
        selectedId === undefined || selectedId === ""
          ? []
          : [
              {
                backgroundColor: selectedItemColor,
                borderWidth: 0,
                drawTime: "beforeDatasetsDraw" as const,
                type: "box" as const,
                xMax: "max" as const,
                xMin: "min" as const,
                yMax: data.findIndex((dataItem) => dataItem.y === selectedId) + 0.5,
                yMin: data.findIndex((dataItem) => dataItem.y === selectedId) - 0.5,
              },
            ],
    },
    legend: {
      display: false,
    },
    tooltip: {
      callbacks: {
        afterBody(tooltipItems: Array<TooltipItem<"bar">>) {
          const taskInstance = data.find((dataItem) => dataItem.y === tooltipItems[0]?.label);
          const startDate = formatDate(taskInstance?.x[0], selectedTimezone);
          const endDate = formatDate(taskInstance?.x[1], selectedTimezone);

          return [
            `${translate("startDate")}: ${startDate}`,
            `${translate("endDate")}: ${endDate}`,
            `${translate("duration")}: ${getDuration(taskInstance?.x[0], taskInstance?.x[1])}`,
          ];
        },
        label(tooltipItem: TooltipItem<"bar">) {
          const { label } = tooltipItem;
          const taskInstance = data.find((dataItem) => dataItem.y === label);

          return `${translate("state")}: ${translate(`states.${taskInstance?.state}`)}`;
        },
      },
    },
  },
  resizeDelay: 100,
  responsive: true,
  scales: {
    x: {
      grid: {
        color: gridColor,
        display: true,
      },
      max:
        data.length > 0
          ? (() => {
              const maxTime = Math.max(...data.map((item) => new Date(item.x[1] ?? "").getTime()));
              const minTime = Math.min(...data.map((item) => new Date(item.x[0] ?? "").getTime()));
              const totalDuration = maxTime - minTime;

              // add 5% to the max time to avoid the last tick being cut off
              return maxTime + totalDuration * 0.05;
            })()
          : formatDate(selectedRun?.end_date, selectedTimezone),
      min:
        data.length > 0
          ? Math.min(...data.map((item) => new Date(item.x[0] ?? "").getTime()))
          : formatDate(selectedRun?.start_date, selectedTimezone),
      position: "top" as const,
      stacked: true,
      ticks: {
        align: "start" as const,
        callback: (value: number | string) => formatDate(value, selectedTimezone, "HH:mm:ss"),
        maxRotation: 8,
        maxTicksLimit: 8,
        minRotation: 8,
      },
      type: "time" as const,
    },
    y: {
      grid: {
        color: gridColor,
        display: true,
      },
      stacked: true,
      ticks: {
        display: false,
      },
    },
  },
});
