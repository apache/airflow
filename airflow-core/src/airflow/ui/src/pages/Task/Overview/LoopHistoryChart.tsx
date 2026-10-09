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
import { Box, HStack, Heading, Stat, useToken } from "@chakra-ui/react";
import { BarElement, CategoryScale, Chart as ChartJS, Legend, LinearScale, Tooltip } from "chart.js";
import annotationPlugin from "chartjs-plugin-annotation";
import { Bar } from "react-chartjs-2";
import { useTranslation } from "react-i18next";
import { useNavigate, useParams } from "react-router-dom";

import { SearchParamsKeys } from "src/constants/searchParams";
import { useLoopHistory } from "src/queries/useLoopHistory";
import { getComputedCSSVariableValue } from "src/theme";
import { median } from "src/utils/median";

ChartJS.register(CategoryScale, LinearScale, BarElement, Legend, Tooltip, annotationPlugin);

export const LoopHistoryChart = ({ groupId }: { readonly groupId: string }) => {
  const { dagId = "" } = useParams();
  const { t: translate } = useTranslation("dag");
  const navigate = useNavigate();
  const { data } = useLoopHistory({ dagId, groupId });
  const [successColor, failedColor, mutedColor] = useToken("colors", [
    "success.solid",
    "failed.solid",
    "fg.muted",
  ]);

  const runs = data?.runs ?? [];

  if (runs.length === 0) {
    return undefined;
  }

  const capHits = runs.filter((run) => run.status === "ran_to_cap");
  const cap = runs.at(-1)?.max_iterations ?? 0;

  return (
    <Box borderRadius={4} borderWidth={1} flex="1 1 400px" maxWidth="900px" minWidth="320px" p={4}>
      <Heading mb={2} size="sm">
        {translate("loop.history.title")}
      </Heading>
      <HStack gap={6} mb={3}>
        <Stat.Root size="sm">
          <Stat.Label>{translate("loop.history.stats.medianIterations")}</Stat.Label>
          <Stat.ValueText>{median(runs.map((run) => run.iterations_ran))}</Stat.ValueText>
        </Stat.Root>
        <Stat.Root size="sm">
          <Stat.Label>{translate("loop.history.stats.capHits")}</Stat.Label>
          <Stat.ValueText>{capHits.length}</Stat.ValueText>
        </Stat.Root>
      </HStack>
      <Box height="240px">
        <Bar
          data={{
            datasets: [
              {
                backgroundColor: runs.map((run) =>
                  getComputedCSSVariableValue(
                    (run.status === "stopped_early" || run.status === "ran_to_cap"
                      ? successColor
                      : run.status === "failed"
                        ? failedColor
                        : mutedColor) ?? "oklch(0.5 0 0)",
                  ),
                ),
                data: runs.map((run) => run.iterations_ran),
                label: translate("loop.history.iterationsAxis"),
              },
            ],
            labels: runs.map((run) => run.run_id),
          }}
          options={{
            maintainAspectRatio: false,
            onClick: (_event, elements) => {
              const run = runs[elements[0]?.index ?? -1];

              if (run !== undefined) {
                const params = new URLSearchParams();

                if (run.loop_region_id !== null && run.loop_region_id !== undefined) {
                  params.set(SearchParamsKeys.LOOP_REGION_ID, run.loop_region_id);
                }
                void navigate(`/dags/${dagId}/runs/${run.run_id}/tasks/group/${groupId}?${params}`);
              }
            },
            plugins: {
              annotation: {
                annotations: {
                  cap: {
                    borderColor: getComputedCSSVariableValue(mutedColor ?? "oklch(0.5 0 0)"),
                    borderDash: [4, 4],
                    borderWidth: 1,
                    label: {
                      content: translate("loop.history.capLabel", { max: cap }),
                      display: true,
                      position: "start",
                    },
                    scaleID: "y",
                    type: "line",
                    value: cap,
                  },
                },
              },
              legend: { display: false },
            },
            responsive: true,
            scales: {
              x: { ticks: { display: false } },
              y: {
                beginAtZero: true,
                suggestedMax: cap,
                ticks: { precision: 0 },
                title: { display: true, text: translate("loop.history.iterationsAxis") },
              },
            },
          }}
        />
      </Box>
    </Box>
  );
};
