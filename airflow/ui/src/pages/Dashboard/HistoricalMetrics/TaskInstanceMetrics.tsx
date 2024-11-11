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
import type { TaskInstanceStateCount } from "openapi-gen/requests/types.gen";

import { MetricSection } from "./MetricSection";
import { MetricsBadge } from "./MetricsBadge";

type TaskInstanceMetricsProps = {
  readonly taskInstanceStates: TaskInstanceStateCount;
  readonly total: number;
};

const TASK_STATES: Array<keyof TaskInstanceStateCount> = [
  "queued",
  "running",
  "success",
  "failed",
  "skipped",
];

export const TaskInstanceMetrics = ({
  taskInstanceStates,
  total,
}: TaskInstanceMetricsProps) => (
  <Box borderRadius={5} borderWidth={1} mt={2} p={2}>
    <HStack mb={2}>
      <MetricsBadge color="blue.solid" runs={total} />
      <Heading>Task Instances</Heading>
    </HStack>

    {TASK_STATES.map((state) => (
      <MetricSection
        key={state}
        runs={taskInstanceStates[state]}
        state={state}
        total={total}
      />
    ))}
  </Box>
);
