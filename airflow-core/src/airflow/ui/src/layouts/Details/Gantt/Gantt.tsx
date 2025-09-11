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
import { useMemo, useRef, useDeferredValue } from "react";
import { Bar } from "react-chartjs-2";
import { useTranslation } from "react-i18next";
import { useParams, useNavigate, useLocation } from "react-router-dom";

import { useTaskInstanceServiceGetTaskInstances } from "openapi/queries";
import { useColorMode } from "src/context/colorMode";
import { useOpenGroups } from "src/context/openGroups";
import { useTimezone } from "src/context/timezone";
import { flattenNodes } from "src/layouts/Details/Grid/utils";
import { useGridRuns } from "src/queries/useGridRuns";
import { useGridStructure } from "src/queries/useGridStructure";
import { useGridTiSummaries } from "src/queries/useGridTISummaries";
import { getComputedCSSVariableValue } from "src/theme";
import { isStatePending, useAutoRefresh } from "src/utils";
import { formatDate } from "src/utils/datetimeUtils";

import { createHandleBarClick, createChartOptions } from "./utils";

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
  readonly limit: number;
};

const CHART_PADDING = 36;
const CHART_ROW_HEIGHT = 20;
const MIN_BAR_WIDTH = 10;

export const Gantt = ({ limit }: Props) => {
  const { dagId = "", groupId: selectedGroupId, runId = "", taskId: selectedTaskId } = useParams();
  const { openGroupIds } = useOpenGroups();
  const deferredOpenGroupIds = useDeferredValue(openGroupIds);
  const { t: translate } = useTranslation("common");
  const { selectedTimezone } = useTimezone();
  const { colorMode } = useColorMode();
  const navigate = useNavigate();
  const location = useLocation();
  const ref = useRef();

  const [lightGridColor, darkGridColor, lightSelectedColor, darkSelectedColor] = useToken("colors", [
    "gray.200",
    "gray.800",
    "brand.200",
    "brand.800",
  ]);
  const gridColor = colorMode === "light" ? lightGridColor : darkGridColor;
  const selectedItemColor = colorMode === "light" ? lightSelectedColor : darkSelectedColor;

  const { data: gridRuns, isLoading: runsLoading } = useGridRuns({ limit });
  const { data: dagStructure, isLoading: structureLoading } = useGridStructure({ limit });
  const selectedRun = gridRuns?.find((run) => run.run_id === runId);
  const refetchInterval = useAutoRefresh({ dagId });

  // Get grid summaries for groups (which have min/max times)
  const { data: gridTiSummaries, isLoading: summariesLoading } = useGridTiSummaries({
    dagId,
    runId,
    state: selectedRun?.state,
  });

  // Get individual task instances for tasks (which have start/end times)
  const { data: taskInstancesData, isLoading: tiLoading } = useTaskInstanceServiceGetTaskInstances(
    {
      dagId,
      dagRunId: runId,
    },
    undefined,
    {
      enabled: Boolean(dagId),
      refetchInterval: (query) =>
        query.state.data?.task_instances.some((ti) => isStatePending(ti.state)) ? refetchInterval : false,
    },
  );

  const { flatNodes } = useMemo(
    () => flattenNodes(dagStructure, deferredOpenGroupIds),
    [dagStructure, deferredOpenGroupIds],
  );

  const isLoading = runsLoading || structureLoading || summariesLoading || tiLoading;

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
              formatDate(gridSummary.min_start_date, selectedTimezone, "YYYY-MM-DD HH:mm:ss.SSS"),
              formatDate(gridSummary.max_end_date, selectedTimezone, "YYYY-MM-DD HH:mm:ss.SSS"),
            ],
            y: gridSummary.task_id,
          };
        } else if (!node.isGroup) {
          // Individual task - use individual task instance data
          const taskInstance = taskInstances.find((ti) => ti.task_id === node.id);

          if (taskInstance) {
            return {
              isGroup: node.isGroup,
              isMapped: node.is_mapped,
              state: taskInstance.state,
              taskId: taskInstance.task_id,
              x: [
                formatDate(taskInstance.start_date, selectedTimezone, "YYYY-MM-DD HH:mm:ss.SSS"),
                formatDate(taskInstance.end_date, selectedTimezone, "YYYY-MM-DD HH:mm:ss.SSS"),
              ],
              y: taskInstance.task_id,
            };
          }
        }

        return undefined;
      })
      .filter((item) => item !== undefined);
  }, [flatNodes, gridTiSummaries, taskInstancesData, selectedTimezone, isLoading, runId]);

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
          data,
          maxBarThickness: CHART_ROW_HEIGHT,
          minBarLength: MIN_BAR_WIDTH,
        },
      ],
      labels: flatNodes.map((node) => node.id),
    }),
    [data, flatNodes, stateColorMap],
  );

  const fixedHeight = flatNodes.length * CHART_ROW_HEIGHT + CHART_PADDING;
  const selectedId = selectedTaskId ?? selectedGroupId;

  const handleBarClick = useMemo(
    () => createHandleBarClick({ dagId, data, location, navigate, runId }),
    [data, dagId, runId, navigate, location],
  );

  const chartOptions = useMemo(
    () =>
      createChartOptions({
        data,
        gridColor,
        handleBarClick,
        selectedId,
        selectedItemColor,
        selectedRun,
        selectedTimezone,
        translate,
      }),
    [
      data,
      selectedId,
      selectedItemColor,
      gridColor,
      selectedRun,
      selectedTimezone,
      translate,
      handleBarClick,
    ],
  );

  if (runId === "") {
    return undefined;
  }

  return (
    <Box height={`${fixedHeight}px`} minW="250px" ml={-2} mt={36} w="100%">
      <Bar
        data={chartData}
        options={chartOptions}
        ref={ref}
        style={{
          paddingTop: flatNodes.length === 1 ? 15 : 1.5,
        }}
      />
    </Box>
  );
};
