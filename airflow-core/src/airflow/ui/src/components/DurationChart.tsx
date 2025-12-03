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
import { Box, Heading, useToken } from "@chakra-ui/react";
import {
  Chart as ChartJS,
  CategoryScale,
  LinearScale,
  PointElement,
  LineElement,
  BarElement,
  Filler,
  Tooltip,
} from "chart.js";
import type { PartialEventContext } from "chartjs-plugin-annotation";
import annotationPlugin from "chartjs-plugin-annotation";
import dayjs from "dayjs";
import { Bar } from "react-chartjs-2";
import { useTranslation } from "react-i18next";
import { useNavigate } from "react-router-dom";

import type { TaskInstanceResponse, GridRunsResponse } from "openapi/requests/types.gen";
import { getComputedCSSVariableValue } from "src/theme";
import { DEFAULT_DATETIME_FORMAT, renderDuration } from "src/utils/datetimeUtils";

ChartJS.register(
  CategoryScale,
  LinearScale,
  PointElement,
  BarElement,
  LineElement,
  Filler,
  Tooltip,
  annotationPlugin,
);

const average = (ctx: PartialEventContext, index: number) => {
  const values: Array<number> | undefined = ctx.chart.data.datasets[index]?.data as Array<number> | undefined;

  return values === undefined ? 0 : values.reduce((initial, next) => initial + next, 0) / values.length;
};

type RunResponse = GridRunsResponse | TaskInstanceResponse;

const getDuration = (start: string, end: string | null) => {
  const startDate = dayjs(start);
  const endDate = end === null ? dayjs() : dayjs(end);

  if (!startDate.isValid() || !endDate.isValid()) {
    return 0;
  }

  return dayjs.duration(endDate.diff(startDate)).asSeconds();
};

export const DurationChart = ({
  entries,
  kind,
}: {
  readonly entries: Array<RunResponse> | undefined;
  readonly kind: "Dag Run" | "Task Instance";
}) => {
  const { t: translate } = useTranslation(["components", "common"]);
  const navigate = useNavigate();
  const [queuedColorToken] = useToken("colors", ["queued.solid"]);

  // Get states and create color tokens for them
  const states = entries?.map((entry) => entry.state).filter(Boolean) ?? [];
  const stateColorTokens = useToken(
    "colors",
    states.map((state) => `${state}.solid`),
  );

  if (!entries) {
    return undefined;
  }

  // Create a mapping of state to color for easy lookup
  const stateColorMap: Record<string, string> = {};

  states.forEach((state, index) => {
    if (state) {
      stateColorMap[state] = getComputedCSSVariableValue(stateColorTokens[index] ?? "oklch(0.5 0 0)");
    }
  });

  const runAnnotation = {
    borderColor: "grey",
    borderWidth: 1,
    label: {
      content: (ctx: PartialEventContext) => renderDuration(average(ctx, 1), false) ?? "0",
      display: true,
      position: "end",
    },
    scaleID: "y",
    value: (ctx: PartialEventContext) => average(ctx, 1),
  };

  const queuedAnnotation = {
    borderColor: "grey",
    borderWidth: 1,
    label: {
      content: (ctx: PartialEventContext) => renderDuration(average(ctx, 0), false) ?? "0",
      display: true,
      position: "end",
    },
    scaleID: "y",
    value: (ctx: PartialEventContext) => average(ctx, 0),
  };

  return (
    <Box>
      <Heading pb={2} size="sm" textAlign="center">
        {kind === "Dag Run"
          ? translate("durationChart.lastDagRun", { count: entries.length })
          : translate("durationChart.lastTaskInstance", { count: entries.length })}
      </Heading>
      <Bar
        data={{
          datasets: [
            {
              backgroundColor: getComputedCSSVariableValue(queuedColorToken ?? "oklch(0.5 0 0)"),
              data: entries.map((entry: RunResponse) => {
                switch (kind) {
                  case "Dag Run": {
                    const run = entry as GridRunsResponse;

                    return run.queued_at !== null && run.start_date !== null && run.queued_at < run.start_date
                      ? Number(getDuration(run.queued_at, run.start_date))
                      : 0;
                  }
                  case "Task Instance": {
                    const taskInstance = entry as TaskInstanceResponse;

                    return taskInstance.queued_when !== null &&
                      taskInstance.start_date !== null &&
                      taskInstance.queued_when < taskInstance.start_date
                      ? Number(getDuration(taskInstance.queued_when, taskInstance.start_date))
                      : 0;
                  }
                  default:
                    return 0;
                }
              }),
              label: translate("durationChart.queuedDuration"),
            },
            {
              backgroundColor: entries.map(
                (entry: RunResponse) =>
                  (entry.state ? stateColorMap[entry.state] : undefined) ?? "oklch(0.5 0 0)",
              ),
              data: entries.map((entry: RunResponse) =>
                entry.start_date === null ? 0 : Number(getDuration(entry.start_date, entry.end_date)),
              ),
              label: translate("durationChart.runDuration"),
            },
          ],
          labels: entries.map((entry: RunResponse) => dayjs(entry.run_after).format(DEFAULT_DATETIME_FORMAT)),
        }}
        datasetIdKey="id"
        options={{
          onClick: (_event, elements) => {
            const [element] = elements;

            if (!element) {
              return;
            }

            switch (kind) {
              case "Dag Run": {
                const entry = entries[element.index] as GridRunsResponse | undefined;
                const baseUrl = `/dags/${entry?.dag_id}/runs/${entry?.run_id}`;

                navigate(baseUrl);
                break;
              }
              case "Task Instance": {
                const entry = entries[element.index] as TaskInstanceResponse | undefined;
                const baseUrl = `/dags/${entry?.dag_id}/runs/${entry?.dag_run_id}`;

                navigate(`${baseUrl}/tasks/${entry?.task_id}`);
                break;
              }
              default:
            }
          },
          onHover: (_event, elements, chart) => {
            chart.canvas.style.cursor = elements.length > 0 ? "pointer" : "default";
          },
          plugins: {
            annotation: {
              annotations: {
                queuedAnnotation,
                runAnnotation,
              },
            },
            tooltip: {
              callbacks: {
                label: (context) => {
                  const datasetLabel = context.dataset.label ?? "";

                  const formatted = renderDuration(context.parsed.y, false) ?? "0";

                  return datasetLabel ? `${datasetLabel}: ${formatted}` : formatted;
                },
              },
            },
          },
          responsive: true,
          scales: {
            x: {
              stacked: true,
              ticks: {
                maxTicksLimit: 3,
              },
              title: { align: "end", display: true, text: translate("common:dagRun.runAfter") },
            },
            y: {
              ticks: {
                callback: (value) => {
                  const num = typeof value === "number" ? value : Number(value);

                  return renderDuration(num, false) ?? "0";
                },
              },
              title: { align: "end", display: true, text: translate("common:duration") },
            },
          },
        }}
      />
    </Box>
  );
};
