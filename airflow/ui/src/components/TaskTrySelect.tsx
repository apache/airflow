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
import { Button, createListCollection, HStack, VStack, Heading } from "@chakra-ui/react";

import { useTaskInstanceServiceGetMappedTaskInstanceTries } from "openapi/queries";
import type { TaskInstanceHistoryResponse, TaskInstanceResponse } from "openapi/requests/types.gen";
import { StateBadge } from "src/components/StateBadge";
import { useConfig } from "src/queries/useConfig";
import { isStatePending } from "src/utils/refresh";

import TaskInstanceTooltip from "./TaskInstanceTooltip";
import { Select } from "./ui";

type Props = {
  readonly onSelectTryNumber?: (tryNumber: number) => void;
  readonly selectedTryNumber?: number;
  readonly taskInstance: TaskInstanceResponse;
};

export const TaskTrySelect = ({ onSelectTryNumber, selectedTryNumber, taskInstance }: Props) => {
  const {
    dag_id: dagId,
    dag_run_id: dagRunId,
    map_index: mapIndex,
    state,
    task_id: taskId,
    try_number: finalTryNumber,
  } = taskInstance;

  const autoRefreshInterval = useConfig("auto_refresh_interval") as number;

  const { data: tiHistory } = useTaskInstanceServiceGetMappedTaskInstanceTries(
    {
      dagId,
      dagRunId,
      mapIndex,
      taskId,
    },
    undefined,
    {
      enabled: Boolean(finalTryNumber && finalTryNumber > 1), // Only try to look up task tries if try number > 1
      refetchInterval: (query) =>
        // We actually want to use || here
        // eslint-disable-next-line @typescript-eslint/prefer-nullish-coalescing
        query.state.data?.task_instances.some((ti) => isStatePending(ti.state)) || isStatePending(state)
          ? autoRefreshInterval * 1000
          : false,
    },
  );

  if (!finalTryNumber || finalTryNumber <= 1) {
    return undefined;
  }

  const logAttemptDropdownLimit = 10;
  const showDropdown = finalTryNumber > logAttemptDropdownLimit;

  const tryOptions = createListCollection({
    items: (tiHistory?.task_instances ?? []).map((ti) => ({
      task_instance: ti,
      value: ti.try_number.toString(),
    })),
  });

  return (
    <VStack alignItems="flex-start" gap={1} my={3}>
      <Heading size="md">Task Tries</Heading>
      {showDropdown ? (
        <Select.Root
          collection={tryOptions}
          data-testid="select-task-try"
          defaultValue={[finalTryNumber.toString()]}
          onValueChange={(details) => {
            if (onSelectTryNumber) {
              onSelectTryNumber(
                details.value[0] === undefined ? finalTryNumber : parseInt(details.value[0], 10),
              );
            }
          }}
          width="200px"
        >
          <Select.Trigger>
            <Select.ValueText placeholder="Task Try">
              {(
                items: Array<{
                  task_instance: TaskInstanceHistoryResponse;
                  value: number;
                }>,
              ) => <StateBadge state={items[0]?.task_instance.state}>{items[0]?.value}</StateBadge>}
            </Select.ValueText>
          </Select.Trigger>
          <Select.Content flexDirection="column-reverse">
            {tryOptions.items.map((option) => (
              <Select.Item item={option} key={option.value}>
                <span>
                  {option.value}:
                  <StateBadge ml={2} state={option.task_instance.state}>
                    {option.task_instance.state}
                  </StateBadge>
                </span>
              </Select.Item>
            ))}
          </Select.Content>
        </Select.Root>
      ) : (
        <HStack>
          {tiHistory?.task_instances.map((ti) => (
            <TaskInstanceTooltip key={ti.try_number} taskInstance={ti}>
              <Button
                colorPalette="blue"
                data-testid={`log-attempt-select-button-${ti.try_number}`}
                key={ti.try_number}
                onClick={() => {
                  if (onSelectTryNumber && ti.try_number) {
                    onSelectTryNumber(ti.try_number);
                  }
                }}
                variant={selectedTryNumber === ti.try_number ? "surface" : "outline"}
              >
                {ti.try_number}
                <StateBadge state={ti.state} />
              </Button>
            </TaskInstanceTooltip>
          ))}
        </HStack>
      )}
    </VStack>
  );
};
