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
import { Heading, VStack, Box, SimpleGrid, Text, Link } from "@chakra-ui/react";
import { Link as RouterLink } from "react-router-dom";

import type {
  TaskResponse,
  TaskInstanceResponse,
} from "openapi/requests/types.gen";
import TaskInstanceTooltip from "src/components/TaskInstanceTooltip";
import Time from "src/components/Time";
import { Status } from "src/components/ui";

import { TaskRecentRuns } from "./TaskRecentRuns.tsx";

type Props = {
  readonly dagId: string;
  readonly task: TaskResponse;
  readonly taskInstances: Array<TaskInstanceResponse>;
};

export const TaskCard = ({ dagId, task, taskInstances }: Props) => (
  <Box
    borderColor="border.emphasized"
    borderRadius={8}
    borderWidth={1}
    overflow="hidden"
    px={3}
    py={2}
  >
    <Link asChild color="fg.info" fontWeight="bold">
      <RouterLink to={`/dags/${dagId}/tasks/${task.task_id}`}>
        {task.task_display_name ?? task.task_id}
        {task.is_mapped ? "[]" : undefined}
      </RouterLink>
    </Link>
    <SimpleGrid columns={4} gap={4} height={20}>
      <VStack align="flex-start" gap={1}>
        <Heading color="fg.muted" fontSize="xs">
          Operator
        </Heading>
        <Text fontSize="sm">{task.operator_name}</Text>
      </VStack>
      <VStack align="flex-start" gap={1}>
        <Heading color="fg.muted" fontSize="xs">
          Trigger Rule
        </Heading>
        <Text fontSize="sm">{task.trigger_rule}</Text>
      </VStack>
      <VStack align="flex-start" gap={1}>
        <Heading color="fg.muted" fontSize="xs">
          Last Instance
        </Heading>
        {taskInstances[0] ? (
          <TaskInstanceTooltip taskInstance={taskInstances[0]}>
            <Link asChild color="fg.info" fontSize="sm">
              <RouterLink
                to={`/dags/${dagId}/runs/${taskInstances[0].dag_run_id}/tasks/${task.task_id}`}
              >
                <Time datetime={taskInstances[0].start_date} />
                {taskInstances[0].state === null ? undefined : (
                  <Status state={taskInstances[0].state}>
                    {taskInstances[0].state}
                  </Status>
                )}
              </RouterLink>
            </Link>
          </TaskInstanceTooltip>
        ) : undefined}
      </VStack>
      {/* TODO: Handled mapped tasks to not plot each map index as a task instance */}
      {!task.is_mapped && <TaskRecentRuns taskInstances={taskInstances} />}
    </SimpleGrid>
  </Box>
);
