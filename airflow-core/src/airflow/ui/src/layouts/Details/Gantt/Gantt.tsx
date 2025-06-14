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
import { Box } from "@chakra-ui/react";
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
import "chartjs-adapter-dayjs-4";
import annotationPlugin from "chartjs-plugin-annotation";
import dayjs from "dayjs";
import { useMemo, useRef } from "react";
import { Bar } from "react-chartjs-2";
import { useTranslation } from "react-i18next";
import { useParams } from "react-router-dom";

import { useOpenGroups } from "src/context/openGroups";
import { useTimezone } from "src/context/timezone";
import { flattenNodes } from "src/layouts/Details/Grid/utils";
import { useGrid } from "src/queries/useGrid";
import { system } from "src/theme";
import { getDuration } from "src/utils";

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

const CHART_PADDING = 38;
const CHART_ROW_HEIGHT = 20;

export const Gantt = () => {
  const { groupId: selectedGroupId, runId, taskId: selectedTaskId } = useParams();
  const { openGroupIds } = useOpenGroups();
  const { t: translate } = useTranslation("common");
  const { selectedTimezone } = useTimezone();
  const ref = useRef();

  const { data: gridData, isLoading } = useGrid();

  const { flatNodes } = useMemo(
    () => flattenNodes(gridData === undefined ? [] : gridData.structure.nodes, openGroupIds),
    [gridData, openGroupIds],
  );

  const runs = gridData?.dag_runs ?? [];

  const taskInstances = runs.find((run) => run.dag_run_id === runId)?.task_instances;

  if (isLoading || taskInstances === undefined || runId === undefined) {
    return undefined;
  }

  const data = flatNodes.map((node) => {
    const taskInstance = taskInstances.find((ti) => ti.task_id === node.id);

    if (taskInstance === undefined) {
      return undefined;
    }

    return {
      state: taskInstance.state,
      x: [
        dayjs(taskInstance.start_date).tz(selectedTimezone).format("YYYY-MM-DD HH:mm:ss.SSS"),
        dayjs(taskInstance.end_date).tz(selectedTimezone).format("YYYY-MM-DD HH:mm:ss.SSS"),
      ],
      y: taskInstance.task_id,
    };
  });

  const fixedHeight = data.length * CHART_ROW_HEIGHT + CHART_PADDING;

  return (
    <Box height={`${fixedHeight}px`} mt={0.5}>
      <Bar
        data={{
          datasets: [
            {
              backgroundColor: data.map(
                (dataItem) =>
                  system.tokens.categoryMap.get("colors")?.get(`${dataItem?.state}.600`)?.value as string,
              ),
              data,
              maxBarThickness: CHART_ROW_HEIGHT,
            },
          ],
          labels: [],
        }}
        datasetIdKey="id"
        options={{
          animation: {
            duration: 400,
          },
          indexAxis: "y",
          maintainAspectRatio: false,
          plugins: {
            annotation: {
              annotations:
                selectedTaskId === undefined && selectedGroupId === undefined
                  ? []
                  : [
                      {
                        backgroundColor: system.tokens.categoryMap.get("colors")?.get(`blue.300`)
                          ?.value as string,
                        borderWidth: 0,
                        drawTime: "beforeDatasetsDraw",
                        type: "box",
                        xMax: "max",
                        xMin: "min",
                        yMax:
                          selectedTaskId === undefined
                            ? data.findIndex((dataItem) => dataItem?.y === selectedGroupId) + 0.5
                            : data.findIndex((dataItem) => dataItem?.y === selectedTaskId) + 0.5,
                        yMin:
                          selectedTaskId === undefined
                            ? data.findIndex((dataItem) => dataItem?.y === selectedGroupId) - 0.5
                            : data.findIndex((dataItem) => dataItem?.y === selectedTaskId) - 0.5,
                      },
                    ],
            },
            legend: {
              display: false,
            },
            tooltip: {
              callbacks: {
                afterBody(tooltipItems) {
                  const taskInstance = data.find((dataItem) => dataItem?.y === tooltipItems[0]?.label);
                  const startDate = dayjs(taskInstance?.x[0]).format("YYYY-MM-DD HH:mm:ss");
                  const endDate = dayjs(taskInstance?.x[1]).format("YYYY-MM-DD HH:mm:ss");

                  return [
                    `${translate("startDate")}: ${startDate}`,
                    `${translate("endDate")}: ${endDate}`,
                    `${translate("duration")}: ${getDuration(taskInstance?.x[0], taskInstance?.x[1])}`,
                  ];
                },
                label(tooltipItem) {
                  const { label } = tooltipItem;
                  const taskInstance = data.find((dataItem) => dataItem?.y === label);

                  return `${translate("state")}: ${translate(`states.${taskInstance?.state}`)}`;
                },
              },
            },
          },
          responsive: true,
          scales: {
            x: {
              max: dayjs(runs.find((run) => run.dag_run_id === runId)?.end_date)
                .tz(selectedTimezone)
                .format("YYYY-MM-DD HH:mm:ss"),
              min: dayjs(runs.find((run) => run.dag_run_id === runId)?.start_date)
                .tz(selectedTimezone)
                .format("YYYY-MM-DD HH:mm:ss"),
              position: "top",
              stacked: true,
              ticks: {
                align: "start",
                callback: (value) => dayjs(value).tz(selectedTimezone).format("HH:mm:ss"),
                maxTicksLimit: 8,
                minRotation: 14,
              },
              type: "time",
            },
            y: {
              grid: {
                display: true,
              },
              stacked: true,
              ticks: {
                display: false,
              },
            },
          },
        }}
        ref={ref}
      />
    </Box>
  );
};
