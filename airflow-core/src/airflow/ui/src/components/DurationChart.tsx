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
import { Box, Heading } from "@chakra-ui/react";
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
import { useNavigate } from "react-router-dom";

import type { TaskInstanceResponse, GridRunsResponse } from "openapi/requests/types.gen";
import { system } from "src/theme";
import { pluralize } from "src/utils";

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

const getDuration = (start: string, end: string | null) => dayjs.duration(dayjs(end).diff(start)).asSeconds();

export const DurationChart = ({
  entries,
  kind,
}: {
  readonly entries: Array<RunResponse> | undefined;
  readonly kind: "Dag Run" | "Task Instance";
}) => {
  const navigate = useNavigate();

  if (!entries) {
    return undefined;
  }

  const runAnnotation = {
    borderColor: "green",
    borderWidth: 1,
    label: {
      content: (ctx: PartialEventContext) => average(ctx, 1).toFixed(2),
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
      content: (ctx: PartialEventContext) => average(ctx, 0).toFixed(2),
      display: true,
      position: "end",
    },
    scaleID: "y",
    value: (ctx: PartialEventContext) => average(ctx, 0),
  };

  return (
    <Box>
      <Heading pb={2} size="sm" textAlign="center">
        Last {pluralize(kind, entries.length)}
      </Heading>
      <Bar
        data={{
          datasets: [
            {
              backgroundColor: system.tokens.categoryMap.get("colors")?.get("queued.600")?.value as string,
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
              label: "Queued duration",
            },
            {
              backgroundColor: entries.map(
                (entry: RunResponse) =>
                  system.tokens.categoryMap.get("colors")?.get(`${entry.state}.600`)?.value as string,
              ),
              data: entries.map((entry: RunResponse) =>
                entry.start_date === null ? 0 : Number(getDuration(entry.start_date, entry.end_date)),
              ),
              label: "Run duration",
            },
          ],
          labels: entries.map((entry: RunResponse) => dayjs(entry.run_after).format("YYYY-MM-DD, hh:mm:ss")),
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
          },
          responsive: true,
          scales: {
            x: {
              stacked: true,
              ticks: {
                maxTicksLimit: 3,
              },
              title: { align: "end", display: true, text: "Run After" },
            },
            y: {
              title: { align: "end", display: true, text: "Duration (seconds)" },
            },
          },
        }}
      />
    </Box>
  );
};
