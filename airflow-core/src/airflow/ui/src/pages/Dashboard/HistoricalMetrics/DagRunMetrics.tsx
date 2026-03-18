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
import { Box, Separator, Heading, HStack, Stack } from "@chakra-ui/react";
import type { DAGRunStates } from "openapi-gen/requests/types.gen";
import { useTranslation } from "react-i18next";
import { FiBarChart } from "react-icons/fi";

import { MetricSection } from "./MetricSection";

type DagRunMetricsProps = {
  readonly dagRunStates: DAGRunStates;
  readonly endDate?: string;
  readonly startDate: string;
  readonly stateCountLimit: number;
};

const DAGRUN_STATES: Array<keyof DAGRunStates> = ["queued", "running", "success", "failed"];

export const DagRunMetrics = ({ dagRunStates, endDate, startDate, stateCountLimit }: DagRunMetricsProps) => {
  const { t: translate } = useTranslation();
  const total = Object.values(dagRunStates).reduce((sum, count) => sum + count, 0);

  return (
    <Box borderRadius={5} borderWidth={1} p={4}>
      <HStack>
        <FiBarChart />
        <Heading size="md">{translate("dagRun", { count: 2 })}</Heading>
      </HStack>
      <Separator my={3} />
      <Stack gap={4}>
        {DAGRUN_STATES.map((state) => (
          <MetricSection
            capped={dagRunStates[state] >= stateCountLimit}
            endDate={endDate}
            key={state}
            kind="dag_runs"
            runs={dagRunStates[state]}
            startDate={startDate}
            state={state}
            total={total}
          />
        ))}
      </Stack>
    </Box>
  );
};
