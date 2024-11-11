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
import {
  Box,
  Flex,
  Heading,
  HStack,
  VStack,
  Text,
  createListCollection,
  type SelectValueChangeDetails,
} from "@chakra-ui/react";
import dayjs from "dayjs";
import type {
  TaskInstanceStateCount,
  DAGRunStates,
} from "openapi-gen/requests/types.gen";
import { useState } from "react";
import { FiCalendar } from "react-icons/fi";

import { useDashboardServiceHistoricalMetrics } from "openapi/queries";
import { ErrorAlert } from "src/components/ErrorAlert";
import Time from "src/components/Time";
import { Select } from "src/components/ui";
import { capitalize } from "src/utils";
import { stateColor } from "src/utils/stateColor";

const BAR_WIDTH = 100;
const BAR_HEIGHT = 5;
const DAGRUN_STATES: Array<keyof DAGRunStates> = [
  "queued",
  "running",
  "success",
  "failed",
];
const TASK_STATES: Array<keyof TaskInstanceStateCount> = [
  "queued",
  "running",
  "success",
  "failed",
  "skipped",
];
const timeOptions = createListCollection({
  items: [
    { label: "Last 1 hour", value: "1" },
    { label: "Last 8 hours", value: "8" },
    { label: "Last 12 hours", value: "12" },
    { label: "Last 24 hours", value: "24" },
  ],
});

type Props = {
  readonly runs: number;
  readonly state: string;
  readonly total: number;
};

type DagRunStateInfoProps = {
  readonly dagRunStates: DAGRunStates;
  readonly total: number;
};

type TaskRunStateInfoProps = {
  readonly taskRunStates: TaskInstanceStateCount;
  readonly total: number;
};

const StateInfo = ({ runs, state, total }: Props) => {
  // Calculate the given state as a percentage of total and draw a bar
  // in state's color with width as state's percentage and remaining width filed as gray
  const statePercent = total === 0 ? 0 : ((runs / total) * 100).toFixed(2);
  const stateWidth = total === 0 ? 0 : (runs / total) * BAR_WIDTH;
  const remainingWidth = BAR_WIDTH - stateWidth;

  return (
    <VStack align="left" gap={1} mb={4} ml={0} pl={0}>
      <Flex justify="space-between">
        <HStack>
          <Text
            bg={stateColor[state as keyof typeof stateColor]}
            borderRadius={15}
            pb={1}
            pt={1}
            px={5}
          >
            {runs}
          </Text>
          <Text> {capitalize(state)} </Text>
        </HStack>
        <Text color="gray.500"> {statePercent}% </Text>
      </Flex>
      <HStack gap={0} mt={2}>
        <Box
          bg={stateColor[state as keyof typeof stateColor]}
          borderLeftRadius={5}
          height={`${BAR_HEIGHT}px`}
          minHeight={2}
          width={`${stateWidth}%`}
        />
        <Box
          bg={stateColor.queued}
          borderLeftRadius={runs === 0 ? 5 : 0} // When there are no states then have left radius too since this is the only bar displayed
          borderRightRadius={5}
          height={`${BAR_HEIGHT}px`}
          minHeight={2}
          width={`${remainingWidth}%`}
        />
      </HStack>
    </VStack>
  );
};

const DagRunStateInfo = ({ dagRunStates, total }: DagRunStateInfoProps) =>
  DAGRUN_STATES.map((state) => (
    <StateInfo
      key={state}
      runs={dagRunStates[state]}
      state={state}
      total={total}
    />
  ));

const TaskRunStateInfo = ({ taskRunStates, total }: TaskRunStateInfoProps) =>
  TASK_STATES.map((state) => (
    <StateInfo
      key={state}
      runs={taskRunStates[state]}
      state={state}
      total={total}
    />
  ));

export const Metrics = () => {
  const defaultHour = "8";
  const now = dayjs();
  const [startDate, setStartDate] = useState(
    now.subtract(Number(defaultHour), "hour").toISOString(),
  );
  const [endDate, setEndDate] = useState(now.toISOString());

  const { data, error, isLoading } = useDashboardServiceHistoricalMetrics({
    endDate,
    startDate,
  });

  const dagRunTotal = data
    ? Object.values(data.dag_run_states).reduce(
        (partialSum, value) => partialSum + value,
        0,
      )
    : 0;

  const taskRunTotal = data
    ? Object.values(data.task_instance_states).reduce(
        (partialSum, value) => partialSum + value,
        0,
      )
    : 0;

  const handleTimeChange = ({
    value,
  }: SelectValueChangeDetails<Array<string>>) => {
    const cnow = dayjs();

    setStartDate(cnow.subtract(Number(value[0]), "hour").toISOString());
    setEndDate(cnow.toISOString());
  };

  return (
    <Box width="100%">
      <ErrorAlert error={error} />
      <VStack alignItems="left" gap={2}>
        <HStack>
          <FiCalendar />
          <Select.Root
            collection={timeOptions}
            data-testid="sort-by-time"
            defaultValue={[defaultHour]}
            onValueChange={handleTimeChange}
            width="200px"
          >
            <Select.Trigger>
              <Select.ValueText placeholder="Duration" />
            </Select.Trigger>
            <Select.Content>
              {timeOptions.items.map((option) => (
                <Select.Item item={option} key={option.value}>
                  {option.label}
                </Select.Item>
              ))}
            </Select.Content>
          </Select.Root>
          <Text>
            <Time datetime={startDate} /> - <Time datetime={endDate} />
          </Text>
        </HStack>
        {!isLoading && data !== undefined && (
          <Box>
            <Box borderRadius={5} borderWidth={1} p={2}>
              <HStack mb={2}>
                <Text bg="blue.500" borderRadius={20} px={5} py={1}>
                  {dagRunTotal}
                </Text>
                <Heading>Dag Runs</Heading>
              </HStack>
              <DagRunStateInfo
                dagRunStates={data.dag_run_states}
                total={dagRunTotal}
              />
            </Box>
            <Box borderRadius={5} borderWidth={1} mt={2} p={2}>
              <HStack mb={2}>
                <Text bg="blue.500" borderRadius={20} px={5} py={1}>
                  {taskRunTotal}
                </Text>
                <Heading>Task Instances</Heading>
              </HStack>
              <TaskRunStateInfo
                taskRunStates={data.task_instance_states}
                total={taskRunTotal}
              />
            </Box>
          </Box>
        )}
      </VStack>
    </Box>
  );
};
