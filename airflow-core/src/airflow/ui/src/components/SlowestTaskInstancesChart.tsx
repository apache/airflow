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
import { Box, Center, Heading, useToken } from "@chakra-ui/react";
import { Chart as ChartJS, CategoryScale, LinearScale, BarElement, Tooltip, type Plugin } from "chart.js";
import { Bar } from "react-chartjs-2";
import { useTranslation } from "react-i18next";

import type { TaskInstanceResponse } from "openapi/requests/types.gen";
import { getComputedCSSVariableValue } from "src/theme";
import { getDurationTickStep, renderCompactDuration, renderDuration } from "src/utils/datetimeUtils";

ChartJS.register(CategoryScale, LinearScale, BarElement, Tooltip);

const CHART_HEIGHT = "340px";

export const SlowestTaskInstancesChart = ({
  taskInstances,
}: {
  readonly taskInstances: Array<TaskInstanceResponse>;
}) => {
  const { t: translate } = useTranslation(["components", "common"]);
  const [labelColorToken, fallbackColorToken] = useToken("colors", ["fg.muted", "gray.solid"]);

  const states = taskInstances.map((taskInstance) => taskInstance.state).filter(Boolean);
  const stateColorTokens = useToken(
    "colors",
    states.map((state) => `${state}.solid`),
  );

  if (taskInstances.length === 0) {
    return (
      <Box data-testid="slowest-task-instances-chart" height={CHART_HEIGHT}>
        <Heading pb={2} size="sm" textAlign="center">
          {translate("slowestTaskInstances.title")}
        </Heading>
        <Center color="fg.muted" fontSize="sm" height="100%">
          {translate("slowestTaskInstances.empty")}
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

  const fallbackColor = getComputedCSSVariableValue(fallbackColorToken ?? "oklch(0.5 0 0)");
  const durations = taskInstances.map((taskInstance) => taskInstance.duration ?? 0);
  const maxDuration = Math.max(...durations, 0);

  const barEndLabels: Plugin<"bar"> = {
    afterDatasetsDraw: (chart) => {
      const { ctx } = chart;
      const meta = chart.getDatasetMeta(0);

      ctx.save();
      ctx.font = "11px system-ui, sans-serif";
      ctx.fillStyle = getComputedCSSVariableValue(labelColorToken ?? "oklch(0.5 0 0)");
      ctx.textBaseline = "middle";
      meta.data.forEach((bar, index) => {
        const duration = durations[index];

        if (duration !== undefined) {
          ctx.fillText(renderCompactDuration(duration), bar.x + 6, bar.y);
        }
      });
      ctx.restore();
    },
    id: "slowestTaskInstancesBarEndLabels",
  };

  return (
    <Box data-testid="slowest-task-instances-chart">
      <Heading pb={2} size="sm" textAlign="center">
        {translate("slowestTaskInstances.title")}
      </Heading>
      <Box height={CHART_HEIGHT}>
        <Bar
          data={{
            datasets: [
              {
                backgroundColor: taskInstances.map(
                  (taskInstance) =>
                    (taskInstance.state ? stateColorMap[taskInstance.state] : undefined) ?? fallbackColor,
                ),
                borderRadius: 3,
                data: durations,
              },
            ],
            labels: taskInstances.map((taskInstance) => taskInstance.task_display_name),
          }}
          options={{
            indexAxis: "y",
            layout: { padding: { right: 64 } },
            maintainAspectRatio: false,
            plugins: {
              legend: { display: false },
              tooltip: {
                callbacks: {
                  label: (context) => renderDuration(context.parsed.x, false) ?? "0",
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
                  stepSize: getDurationTickStep(maxDuration),
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
