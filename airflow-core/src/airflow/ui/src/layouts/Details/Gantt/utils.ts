/* eslint-disable max-lines */

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
import dayjs from "dayjs";
import type { TFunction } from "i18next";
import type { NavigateFunction, Location } from "react-router-dom";

import type {
  GanttTaskInstance,
  GridRunsResponse,
  LightGridTaskInstanceSummary,
  TaskInstanceState,
} from "openapi/requests";
import { SearchParamsKeys } from "src/constants/searchParams";
import type { GridTask } from "src/layouts/Details/Grid/utils";
import { getDuration, isStatePending } from "src/utils";
import { formatDate } from "src/utils/datetimeUtils";
import { buildTaskInstanceUrl } from "src/utils/links";

export type GanttDataItem = {
  isGroup?: boolean | null;
  isMapped?: boolean | null;
  state?: TaskInstanceState | null;
  taskId: string;
  tryNumber?: number;
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
  handleBarHover: (event: ChartEvent, elements: Array<ActiveElement>) => void;
  hoveredId?: string | null;
  hoveredItemColor?: string;
  labels: Array<string>;
  selectedId?: string;
  selectedItemColor?: string;
  selectedRun?: GridRunsResponse;
  selectedTimezone: string;
  translate: TFunction;
};

type TransformGanttDataParams = {
  allTries: Array<GanttTaskInstance>;
  flatNodes: Array<GridTask>;
  gridSummaries: Array<LightGridTaskInstanceSummary>;
};

export const transformGanttData = ({
  allTries,
  flatNodes,
  gridSummaries,
}: TransformGanttDataParams): Array<GanttDataItem> => {
  // Group tries by task_id
  const triesByTask = new Map<string, Array<GanttTaskInstance>>();

  for (const ti of allTries) {
    const existing = triesByTask.get(ti.task_id) ?? [];

    existing.push(ti);
    triesByTask.set(ti.task_id, existing);
  }

  return flatNodes
    .flatMap((node): Array<GanttDataItem> | undefined => {
      const gridSummary = gridSummaries.find((ti) => ti.task_id === node.id);

      // Handle groups and mapped tasks using grid summary (aggregated min/max times)
      // Use ISO so time scale and bar positions render consistently across browsers
      if ((node.isGroup ?? node.is_mapped) && gridSummary) {
        return [
          {
            isGroup: node.isGroup,
            isMapped: node.is_mapped,
            state: gridSummary.state,
            taskId: gridSummary.task_id,
            x: [
              dayjs(gridSummary.min_start_date).toISOString(),
              dayjs(gridSummary.max_end_date).toISOString(),
            ],
            y: gridSummary.task_id,
          },
        ];
      }

      // Handle individual tasks with all their tries
      if (!node.isGroup) {
        const tries = triesByTask.get(node.id);

        if (tries && tries.length > 0) {
          return tries.map((tryInstance) => {
            const hasTaskRunning = isStatePending(tryInstance.state);
            const endTime = hasTaskRunning ? dayjs().toISOString() : tryInstance.end_date;

            return {
              isGroup: false,
              isMapped: tryInstance.is_mapped,
              state: tryInstance.state,
              taskId: tryInstance.task_id,
              tryNumber: tryInstance.try_number,
              x: [dayjs(tryInstance.start_date).toISOString(), dayjs(endTime).toISOString()],
              y: tryInstance.task_display_name,
            };
          });
        }
      }

      return undefined;
    })
    .filter((item): item is GanttDataItem => item !== undefined);
};

export const createHandleBarClick =
  ({ dagId, data, location, navigate, runId }: HandleBarClickOptions) =>
  (_: ChartEvent, elements: Array<ActiveElement>) => {
    if (elements.length === 0 || !elements[0] || !runId) {
      return;
    }

    const clickedData = data[elements[0].index];

    if (!clickedData) {
      return;
    }

    const { isGroup, isMapped, taskId, tryNumber } = clickedData;

    const taskUrl = buildTaskInstanceUrl({
      currentPathname: location.pathname,
      dagId,
      isGroup: Boolean(isGroup),
      isMapped: Boolean(isMapped),
      runId,
      taskId,
    });

    const searchParams = new URLSearchParams(location.search);
    const isOlderTry =
      tryNumber !== undefined &&
      tryNumber <
        Math.max(...data.filter((item) => item.taskId === taskId).map((item) => item.tryNumber ?? 1));

    if (isOlderTry) {
      searchParams.set(SearchParamsKeys.TRY_NUMBER, tryNumber.toString());
    } else {
      searchParams.delete(SearchParamsKeys.TRY_NUMBER);
    }

    void Promise.resolve(
      navigate(
        {
          pathname: taskUrl,
          search: searchParams.toString(),
        },
        { replace: true },
      ),
    );
  };

export const createHandleBarHover = (
  data: Array<GanttDataItem>,
  setHoveredTaskId: (taskId: string | undefined) => void,
) => {
  let lastHoveredTaskId: string | undefined = undefined;

  return (_: ChartEvent, elements: Array<ActiveElement>) => {
    // Clear previous hover styles
    if (lastHoveredTaskId !== undefined) {
      const previousTasks = document.querySelectorAll<HTMLDivElement>(
        `#${lastHoveredTaskId.replaceAll(".", "-")}`,
      );

      previousTasks.forEach((task) => {
        task.style.backgroundColor = "";
      });
    }

    if (elements.length > 0 && elements[0] && elements[0].index < data.length) {
      const hoveredData = data[elements[0].index];

      if (hoveredData?.taskId !== undefined) {
        lastHoveredTaskId = hoveredData.taskId;
        setHoveredTaskId(hoveredData.taskId);

        // Apply new hover styles
        const tasks = document.querySelectorAll<HTMLDivElement>(
          `#${hoveredData.taskId.replaceAll(".", "-")}`,
        );

        tasks.forEach((task) => {
          task.style.backgroundColor = "var(--chakra-colors-info-subtle)";
        });
      }
    } else {
      lastHoveredTaskId = undefined;
      setHoveredTaskId(undefined);
    }
  };
};

export const createChartOptions = ({
  data,
  gridColor,
  handleBarClick,
  handleBarHover,
  hoveredId,
  hoveredItemColor,
  labels,
  selectedId,
  selectedItemColor,
  selectedRun,
  selectedTimezone,
  translate,
}: ChartOptionsParams) => {
  const isActivePending = isStatePending(selectedRun?.state);
  const effectiveEndDate = isActivePending
    ? dayjs().tz(selectedTimezone).format("YYYY-MM-DD HH:mm:ss")
    : selectedRun?.end_date;

  return {
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

      handleBarHover(event, elements);
    },
    plugins: {
      annotation: {
        annotations: [
          // Selected task annotation
          ...(selectedId === undefined || selectedId === "" || hoveredId === selectedId
            ? []
            : [
                {
                  backgroundColor: selectedItemColor,
                  borderWidth: 0,
                  drawTime: "beforeDatasetsDraw" as const,
                  type: "box" as const,
                  xMax: "max" as const,
                  xMin: "min" as const,
                  yMax: labels.indexOf(selectedId) + 0.5,
                  yMin: labels.indexOf(selectedId) - 0.5,
                },
              ]),
          // Hovered task annotation
          ...(hoveredId === null || hoveredId === undefined
            ? []
            : [
                {
                  backgroundColor: hoveredItemColor,
                  borderWidth: 0,
                  drawTime: "beforeDatasetsDraw" as const,
                  type: "box" as const,
                  xMax: "max" as const,
                  xMin: "min" as const,
                  yMax: labels.indexOf(hoveredId) + 0.5,
                  yMin: labels.indexOf(hoveredId) - 0.5,
                },
              ]),
        ],
        clip: false,
      },
      legend: {
        display: false,
      },
      tooltip: {
        callbacks: {
          afterBody(tooltipItems: Array<TooltipItem<"bar">>) {
            const taskInstance = data[tooltipItems[0]?.dataIndex ?? 0];
            const startDate = formatDate(taskInstance?.x[0], selectedTimezone);
            const endDate = formatDate(taskInstance?.x[1], selectedTimezone);
            const lines = [
              `${translate("startDate")}: ${startDate}`,
              `${translate("endDate")}: ${endDate}`,
              `${translate("duration")}: ${getDuration(taskInstance?.x[0], taskInstance?.x[1])}`,
            ];

            if (taskInstance?.tryNumber !== undefined) {
              lines.unshift(`${translate("tryNumber")}: ${taskInstance.tryNumber}`);
            }

            return lines;
          },
          label(tooltipItem: TooltipItem<"bar">) {
            const taskInstance = data[tooltipItem.dataIndex];

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
            : formatDate(effectiveEndDate, selectedTimezone),
        min:
          data.length > 0
            ? (() => {
                const maxTime = Math.max(...data.map((item) => new Date(item.x[1] ?? "").getTime()));
                const minTime = Math.min(...data.map((item) => new Date(item.x[0] ?? "").getTime()));
                const totalDuration = maxTime - minTime;

                // subtract 2% from min time so background color shows before data
                return minTime - totalDuration * 0.02;
              })()
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
  };
};
