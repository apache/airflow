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
import { Box, Heading, HStack } from "@chakra-ui/react";
import type { TaskInstanceState, TaskInstanceStateCount } from "openapi-gen/requests/types.gen";
import { MdOutlineTask } from "react-icons/md";

import { StateBadge } from "src/components/StateBadge";

import { MetricSection } from "./MetricSection";

type TaskInstanceMetricsProps = {
  readonly taskInstanceStates: TaskInstanceStateCount;
  readonly total: number;
  readonly startDate: string;
  readonly endDate: string;
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
];

export const TaskInstanceMetrics = ({
  taskInstanceStates,
  total,
  startDate,
  endDate,
}: TaskInstanceMetricsProps) => (
  <Box borderRadius={5} borderWidth={1} mt={2} p={2}>
    <HStack mb={4}>
      <StateBadge colorPalette="blue" fontSize="md" variant="solid">
        <MdOutlineTask />
        {total}
      </StateBadge>
      <Heading size="md">Task Instances</Heading>
    </HStack>
    {TASK_STATES.sort((stateA, stateB) =>
      taskInstanceStates[stateA] > taskInstanceStates[stateB] ? -1 : 1,
    ).map((state) =>
      taskInstanceStates[state] > 0 ? (
        <MetricSection
          key={state}
          runs={taskInstanceStates[state]}
          state={state as TaskInstanceState}
          total={total}
          startDate={startDate}
          endDate={endDate}
          kind={"task_instances"}
        />
      ) : undefined,
    )}
  </Box>
);
