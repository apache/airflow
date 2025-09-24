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
import { Box, useToken } from "@chakra-ui/react";
import {
  Chart as ChartJS,
  CategoryScale,
  LinearScale,
  PointElement,
  LineElement,
  BarElement,
  Filler,
  Title,
  Tooltip,
  Legend,
  TimeScale,
} from "chart.js";
import "chart.js/auto";
import "chartjs-adapter-dayjs-4/dist/chartjs-adapter-dayjs-4.esm";
import annotationPlugin from "chartjs-plugin-annotation";
import dayjs from "dayjs";
import { useMemo, useDeferredValue } from "react";
import { Bar } from "react-chartjs-2";
import { useTranslation } from "react-i18next";
import { useParams, useNavigate, useLocation } from "react-router-dom";

import { useTaskInstanceServiceGetTaskInstances } from "openapi/queries";
import type { DagRunState, DagRunType } from "openapi/requests/types.gen";
import { useColorMode } from "src/context/colorMode";
import { useHover } from "src/context/hover";
import { useOpenGroups } from "src/context/openGroups";
import { useTimezone } from "src/context/timezone";
import { flattenNodes } from "src/layouts/Details/Grid/utils";
import { useGridRuns } from "src/queries/useGridRuns";
import { useGridStructure } from "src/queries/useGridStructure";
import { useGridTiSummaries } from "src/queries/useGridTISummaries";
import { getComputedCSSVariableValue } from "src/theme";
import { isStatePending, useAutoRefresh } from "src/utils";
import { DEFAULT_DATETIME_FORMAT_WITH_TZ, formatDate } from "src/utils/datetimeUtils";

import { createHandleBarClick, createHandleBarHover, createChartOptions } from "./utils";

ChartJS.register(
  CategoryScale,
  LinearScale,
  PointElement,
  BarElement,
  LineElement,
  Filler,
  Title,
  Tooltip,
  Legend,
  annotationPlugin,
  TimeScale,
);

type Props = {
  readonly dagRunState?: DagRunState | undefined;
  readonly limit: number;
  readonly runType?: DagRunType | undefined;
  readonly triggeringUser?: string | undefined;
};

const CHART_PADDING = 36;
const CHART_ROW_HEIGHT = 20;
const MIN_BAR_WIDTH = 10;

export const Gantt = ({ dagRunState, limit, runType, triggeringUser }: Props) => {
  const { dagId = "", groupId: selectedGroupId, runId = "", taskId: selectedTaskId } = useParams();
  const { openGroupIds } = useOpenGroups();
  const deferredOpenGroupIds = useDeferredValue(openGroupIds);
  const { t: translate } = useTranslation("common");
  const { selectedTimezone } = useTimezone();
  const { colorMode } = useColorMode();
  const { hoveredTaskId, setHoveredTaskId } = useHover();
  const navigate = useNavigate();
  const location = useLocation();

  const [
    lightGridColor,
    darkGridColor,
    lightSelectedColor,
    darkSelectedColor,
    lightHoverColor,
    darkHoverColor,
  ] = useToken("colors", ["gray.200", "gray.800", "blue.200", "blue.800", "blue.100", "blue.900"]);
  const gridColor = colorMode === "light" ? lightGridColor : darkGridColor;
  const selectedItemColor = colorMode === "light" ? lightSelectedColor : darkSelectedColor;
  const hoveredItemColor = colorMode === "light" ? lightHoverColor : darkHoverColor;

  const { data: gridRuns, isLoading: runsLoading } = useGridRuns({
    dagRunState,
    limit,
    runType,
    triggeringUser,
  });
  const { data: dagStructure, isLoading: structureLoading } = useGridStructure({
    dagRunState,
    limit,
    runType,
    triggeringUser,
  });
  const selectedRun = gridRuns?.find((run) => run.run_id === runId);
  const refetchInterval = useAutoRefresh({ dagId });

  // Get grid summaries for groups (which have min/max times)
  const { data: gridTiSummaries, isLoading: summariesLoading } = useGridTiSummaries({
    dagId,
    enabled: Boolean(selectedRun),
    runId,
    state: selectedRun?.state,
  });

  // Get non mapped task instances for tasks (which have start/end times)
  const { data: taskInstancesData, isLoading: tiLoading } = useTaskInstanceServiceGetTaskInstances(
    {
      dagId,
      dagRunId: runId,
      mapIndex: [-1],
    },
    undefined,
    {
      enabled: Boolean(dagId) && Boolean(runId) && Boolean(selectedRun),
      refetchInterval: (query) =>
        query.state.data?.task_instances.some((ti) => isStatePending(ti.state)) ? refetchInterval : false,
    },
  );

  const { flatNodes } = useMemo(
    () => flattenNodes(dagStructure, deferredOpenGroupIds),
    [dagStructure, deferredOpenGroupIds],
  );

  const isLoading = runsLoading || structureLoading || summariesLoading || tiLoading;

  const currentTime = dayjs().tz(selectedTimezone).format(DEFAULT_DATETIME_FORMAT_WITH_TZ);

  const data = useMemo(() => {
    if (isLoading || runId === "") {
      return [];
    }

    const gridSummaries = gridTiSummaries?.task_instances ?? [];
    const taskInstances = taskInstancesData?.task_instances ?? [];

    return flatNodes
      .map((node) => {
        const gridSummary = gridSummaries.find((ti) => ti.task_id === node.id);

        if ((node.isGroup ?? node.is_mapped) && gridSummary) {
          // Use min/max times from grid summary
          return {
            isGroup: node.isGroup,
            isMapped: node.is_mapped,
            state: gridSummary.state,
            taskId: gridSummary.task_id,
            x: [
              formatDate(gridSummary.min_start_date, selectedTimezone, DEFAULT_DATETIME_FORMAT_WITH_TZ),
              formatDate(gridSummary.max_end_date, selectedTimezone, DEFAULT_DATETIME_FORMAT_WITH_TZ),
            ],
            y: gridSummary.task_id,
          };
        } else if (!node.isGroup) {
          // Individual task - use individual task instance data
          const taskInstance = taskInstances.find((ti) => ti.task_id === node.id);

          if (taskInstance) {
            const hasTaskRunning = isStatePending(taskInstance.state);
            const endTime = hasTaskRunning ? currentTime : taskInstance.end_date;

            return {
              isGroup: node.isGroup,
              isMapped: node.is_mapped,
              state: taskInstance.state,
              taskId: taskInstance.task_id,
              x: [
                formatDate(taskInstance.start_date, selectedTimezone, DEFAULT_DATETIME_FORMAT_WITH_TZ),
                formatDate(endTime, selectedTimezone, DEFAULT_DATETIME_FORMAT_WITH_TZ),
              ],
              y: taskInstance.task_id,
            };
          }
        }

        return undefined;
      })
      .filter((item) => item !== undefined);
  }, [flatNodes, gridTiSummaries, taskInstancesData, selectedTimezone, isLoading, runId, currentTime]);

  // Get all unique states and their colors
  const states = [...new Set(data.map((item) => item.state ?? "none"))];
  const stateColorTokens = useToken(
    "colors",
    states.map((state) => `${state}.solid`),
  );
  const stateColorMap = Object.fromEntries(
    states.map((state, index) => [
      state,
      getComputedCSSVariableValue(stateColorTokens[index] ?? "oklch(0.5 0 0)"),
    ]),
  );

  const chartData = useMemo(
    () => ({
      datasets: [
        {
          backgroundColor: data.map((dataItem) => stateColorMap[dataItem.state ?? "none"]),
          data: Boolean(selectedRun) ? data : [],
          maxBarThickness: CHART_ROW_HEIGHT,
          minBarLength: MIN_BAR_WIDTH,
        },
      ],
      labels: flatNodes.map((node) => node.id),
    }),
    [data, flatNodes, stateColorMap, selectedRun],
  );

  const fixedHeight = flatNodes.length * CHART_ROW_HEIGHT + CHART_PADDING;
  const selectedId = selectedTaskId ?? selectedGroupId;

  const handleBarClick = useMemo(
    () => createHandleBarClick({ dagId, data, location, navigate, runId }),
    [data, dagId, runId, navigate, location],
  );

  const handleBarHover = useMemo(
    () => createHandleBarHover(data, setHoveredTaskId),
    [data, setHoveredTaskId],
  );

  const chartOptions = useMemo(
    () =>
      createChartOptions({
        data,
        gridColor,
        handleBarClick,
        handleBarHover,
        hoveredId: hoveredTaskId,
        hoveredItemColor,
        selectedId,
        selectedItemColor,
        selectedRun,
        selectedTimezone,
        translate,
      }),
    [
      data,
      hoveredTaskId,
      hoveredItemColor,
      selectedId,
      selectedItemColor,
      gridColor,
      selectedRun,
      selectedTimezone,
      translate,
      handleBarClick,
      handleBarHover,
    ],
  );

  if (runId === "") {
    return undefined;
  }

  const handleChartMouseLeave = () => {
    setHoveredTaskId(undefined);

    // Clear all hover styles when mouse leaves the chart area
    const allTasks = document.querySelectorAll<HTMLDivElement>('[id*="-"]');

    allTasks.forEach((task) => {
      task.style.backgroundColor = "";
    });
  };

  return (
    <Box height={`${fixedHeight}px`} minW="250px" ml={-2} onMouseLeave={handleChartMouseLeave} w="100%">
      <Bar
        data={chartData}
        options={chartOptions}
        style={{
          paddingTop: flatNodes.length === 1 ? 15 : 1.5,
        }}
      />
    </Box>
  );
};
