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
import type { TaskInstanceState, TaskInstanceStateCount } from "openapi-gen/requests/types.gen";
import { MdOutlineTask } from "react-icons/md";
import { Link as RouterLink } from "react-router-dom";

import { StateBadge } from "src/components/StateBadge";

import { MetricSection } from "./MetricSection";

type TaskInstanceMetricsProps = {
  readonly endDate?: string;
  readonly startDate: string;
  readonly taskInstanceStates: TaskInstanceStateCount;
  readonly total: number;
};

const TASK_STATES: Array<keyof TaskInstanceStateCount> = [
  "queued",
  "running",
  "success",
  "failed",
  "skipped",
  "removed",
  "scheduled",
  "restarting",
  "up_for_retry",
  "up_for_reschedule",
  "upstream_failed",
  "deferred",
  "no_status",
];

export const TaskInstanceMetrics = ({
  endDate,
  startDate,
  taskInstanceStates,
  total,
}: TaskInstanceMetricsProps) => (
  <Box borderRadius={5} borderWidth={1} mt={2} p={4}>
    <HStack>
      <RouterLink
        to={`/task_instances?start_date=${startDate}${endDate === undefined ? "" : `&end_date=${endDate}`}`}
      >
        <StateBadge colorPalette="blue" fontSize="md" variant="solid">
          <MdOutlineTask />
          {total}
        </StateBadge>
      </RouterLink>
      <Heading size="md">Task Instances</Heading>
    </HStack>
    <Separator my={3} />
    <Stack gap={4}>
      {TASK_STATES.sort((stateA, stateB) =>
        taskInstanceStates[stateA] > taskInstanceStates[stateB] ? -1 : 1,
      ).map((state) =>
        taskInstanceStates[state] > 0 ? (
          <MetricSection
            endDate={endDate}
            key={state}
            kind="task_instances"
            runs={taskInstanceStates[state]}
            startDate={startDate}
            state={state as TaskInstanceState}
            total={total}
          />
        ) : undefined,
      )}
    </Stack>
  </Box>
);
