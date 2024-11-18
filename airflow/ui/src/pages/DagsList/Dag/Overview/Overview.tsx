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
import { Box, HStack, Badge, Text, Skeleton } from "@chakra-ui/react";
import dayjs from "dayjs";
import { useState } from "react";
import { Link, useLocation, useParams } from "react-router-dom";

import { useTaskInstanceServiceGetTaskInstances } from "openapi/queries";
import TimeRangeSelector from "src/components/TimeRangeSelector";
import { pluralize } from "src/utils";
import { stateColor } from "src/utils/stateColor";

import { Chart } from "./Chart";

const defaultHour = "8";

export const Overview = () => {
  const { dagId } = useParams();

  const now = dayjs();
  const [startDate, setStartDate] = useState(
    now.subtract(Number(defaultHour), "hour").toISOString(),
  );
  const [endDate, setEndDate] = useState(now.toISOString());

  const { data: failedTasks, isLoading } =
    useTaskInstanceServiceGetTaskInstances({
      dagId: dagId ?? "",
      dagRunId: "~",
      logicalDateGte: startDate,
      logicalDateLte: endDate,
      state: ["failed"],
    });

  const location = useLocation();

  // TODO actually link to task instances list
  return (
    <Box m={4}>
      <Box my={2}>
        <TimeRangeSelector
          defaultValue={defaultHour}
          endDate={endDate}
          setEndDate={setEndDate}
          setStartDate={setStartDate}
          startDate={startDate}
        />
      </Box>
      {failedTasks?.total_entries !== undefined &&
      failedTasks.total_entries > 0 ? (
        // TODO: make sure url params pass correctly
        <Link to={`${location.pathname}/tasks?state=failed`}>
          <HStack borderRadius={4} borderWidth={1} p={3} width="max-content">
            <Badge
              borderRadius="50%"
              colorPalette={stateColor.failed}
              variant="solid"
            >
              {failedTasks.total_entries}
            </Badge>
            <Text fontSize="sm" fontWeight="bold">
              Failed{" "}
              {pluralize("Task", failedTasks.total_entries, undefined, true)}
            </Text>
            <Chart
              endDate={endDate}
              events={failedTasks.task_instances.map((ti) => ({
                timestamp: ti.start_date ?? ti.logical_date,
              }))}
              startDate={startDate}
            />
          </HStack>
        </Link>
      ) : undefined}
      {isLoading ? (
        <Skeleton borderRadius={4} height="45px" width="350px" />
      ) : undefined}
    </Box>
  );
};
