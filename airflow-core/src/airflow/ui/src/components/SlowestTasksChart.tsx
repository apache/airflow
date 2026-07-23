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
import { Box, Center, Heading, Text, useToken } from "@chakra-ui/react";
import { Chart as ChartJS, CategoryScale, LinearScale, BarElement, Tooltip, type Plugin } from "chart.js";
import { Bar } from "react-chartjs-2";
import { useTranslation } from "react-i18next";

import { getComputedCSSVariableValue } from "src/theme";
import { getDurationTickStep, renderCompactDuration } from "src/utils/datetimeUtils";
import type { TaskDurationSummary } from "src/utils/slowestTasks";

ChartJS.register(CategoryScale, LinearScale, BarElement, Tooltip);

const CHART_HEIGHT = "340px";

export const SlowestTasksChart = ({ tasks }: { readonly tasks: Array<TaskDurationSummary> }) => {
  const { t: translate } = useTranslation(["components", "common"]);
  const [labelColorToken] = useToken("colors", ["fg.muted"]);

  const states = tasks.map((task) => task.latestState).filter(Boolean);
  const stateColorTokens = useToken(
    "colors",
    states.map((state) => `${state}.solid`),
  );

  if (tasks.length === 0) {
    return (
      <Box data-testid="slowest-tasks-chart" height={CHART_HEIGHT}>
        <Heading pb={1} size="sm" textAlign="center">
          {translate("slowestTasks.title")}
        </Heading>
        <Center color="fg.muted" fontSize="sm" height="100%">
          {translate("slowestTasks.empty")}
        </Center>
      </Box>
    );
  }

  const stateColorMap: Record<string, string> = {};

  states.forEach((state, index) => {
    if (state) {
      stateColorMap[state] = getComputedCSSVariableValue(stateColorTokens[index] ?? "oklch(0.5 0 0)");
    }
  });

  const barEndLabels: Plugin<"bar"> = {
    afterDatasetsDraw: (chart) => {
      const { ctx } = chart;
      const meta = chart.getDatasetMeta(0);

      ctx.save();
      ctx.font = "11px system-ui, sans-serif";
      ctx.fillStyle = getComputedCSSVariableValue(labelColorToken ?? "oklch(0.5 0 0)");
      ctx.textBaseline = "middle";
      meta.data.forEach((bar, index) => {
        const task = tasks[index];

        if (task !== undefined) {
          ctx.fillText(renderCompactDuration(task.medianDuration), bar.x + 6, bar.y);
        }
      });
      ctx.restore();
    },
    id: "slowestTasksBarEndLabels",
  };

  const maxMedian = Math.max(...tasks.map((task) => task.medianDuration), 0);

  return (
    <Box data-testid="slowest-tasks-chart">
      <Heading pb={1} size="sm" textAlign="center">
        {translate("slowestTasks.title")}
      </Heading>
      <Text color="fg.muted" fontSize="xs" pb={2} textAlign="center">
        {translate("slowestTasks.subtitle")}
      </Text>
      <Box height={CHART_HEIGHT}>
        <Bar
          data={{
            datasets: [
              {
                backgroundColor: tasks.map(
                  (task) =>
                    (task.latestState ? stateColorMap[task.latestState] : undefined) ?? "oklch(0.5 0 0)",
                ),
                borderRadius: 3,
                data: tasks.map((task) => task.medianDuration),
              },
            ],
            labels: tasks.map((task) => task.taskDisplayName),
          }}
          options={{
            indexAxis: "y",
            layout: { padding: { right: 64 } },
            maintainAspectRatio: false,
            plugins: {
              legend: { display: false },
              tooltip: {
                callbacks: {
                  label: (context) => {
                    const task = tasks[context.dataIndex];

                    if (task === undefined) {
                      return "";
                    }

                    return translate("slowestTasks.tooltip", {
                      count: task.runCount,
                      duration: renderCompactDuration(task.medianDuration),
                    });
                  },
                },
              },
            },
            responsive: true,
            scales: {
              x: {
                beginAtZero: true,
                ticks: {
                  callback: (value) =>
                    renderCompactDuration(typeof value === "number" ? value : Number(value)),
                  stepSize: getDurationTickStep(maxMedian),
                },
                title: { align: "end", display: true, text: translate("common:duration") },
              },
              y: { ticks: { autoSkip: false, font: { size: 11 } } },
            },
          }}
          plugins={[barEndLabels]}
        />
      </Box>
    </Box>
  );
};
