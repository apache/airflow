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
  HStack,
  VStack,
  Text,
  createListCollection,
  type SelectValueChangeDetails,
} from "@chakra-ui/react";
import dayjs from "dayjs";
import { useState } from "react";
import { FiCalendar } from "react-icons/fi";

import { useDashboardServiceHistoricalMetrics } from "openapi/queries";
import { ErrorAlert } from "src/components/ErrorAlert";
import Time from "src/components/Time";
import { Select } from "src/components/ui";

import { DagRunMetrics } from "./DagRunMetrics";
import { TaskInstanceMetrics } from "./TaskInstanceMetrics";

const timeOptions = createListCollection({
  items: [
    { label: "Last 1 hour", value: "1" },
    { label: "Last 8 hours", value: "8" },
    { label: "Last 12 hours", value: "12" },
    { label: "Last 24 hours", value: "24" },
  ],
});

export const HistoricalMetrics = () => {
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
            <DagRunMetrics
              dagRunStates={data.dag_run_states}
              total={dagRunTotal}
            />
            <TaskInstanceMetrics
              taskInstanceStates={data.task_instance_states}
              total={taskRunTotal}
            />
          </Box>
        )}
      </VStack>
    </Box>
  );
};
