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
import { Box, HStack, Skeleton, SimpleGrid } from "@chakra-ui/react";
import dayjs from "dayjs";
import { useState } from "react";
import { useTranslation } from "react-i18next";
import { useParams } from "react-router-dom";

import { useTaskInstanceServiceGetTaskInstances } from "openapi/queries";
import { DurationChart } from "src/components/DurationChart";
import { NeedsReviewButton } from "src/components/NeedsReviewButton";
import TimeRangeSelector from "src/components/TimeRangeSelector";
import { TrendCountButton } from "src/components/TrendCountButton";
import { SearchParamsKeys } from "src/constants/searchParams";
import { isStatePending, useAutoRefresh } from "src/utils";

const defaultHour = "24";

export const Overview = () => {
  const { dagId = "", groupId, taskId } = useParams();
  const { t: translate } = useTranslation("dag");

  const now = dayjs();
  const [startDate, setStartDate] = useState(now.subtract(Number(defaultHour), "hour").toISOString());
  const [endDate, setEndDate] = useState(now.toISOString());

  const refetchInterval = useAutoRefresh({});

  const { data: failedTaskInstances, isLoading: isFailedTaskInstancesLoading } =
    useTaskInstanceServiceGetTaskInstances({
      dagId,
      dagRunId: "~",
      limit: 14,
      runAfterGte: startDate,
      runAfterLte: endDate,
      state: ["failed"],
      taskGroup: groupId ?? undefined,
      taskId: Boolean(groupId) ? undefined : taskId,
    });

  const { data: tiData, isLoading: isLoadingTaskInstances } = useTaskInstanceServiceGetTaskInstances(
    {
      dagId,
      dagRunId: "~",
      limit: 14,
      orderBy: ["-run_after"],
      taskGroup: groupId ?? undefined,
      taskId: Boolean(groupId) ? undefined : taskId,
    },
    undefined,
    {
      refetchInterval: (query) =>
        query.state.data?.task_instances.some((ti) => isStatePending(ti.state)) ? refetchInterval : false,
    },
  );

  return (
    <Box m={4} spaceY={4}>
      <NeedsReviewButton taskId={taskId} />
      <Box my={2}>
        <TimeRangeSelector
          defaultValue={defaultHour}
          endDate={endDate}
          setEndDate={setEndDate}
          setStartDate={setStartDate}
          startDate={startDate}
        />
      </Box>
      <HStack flexWrap="wrap">
        <TrendCountButton
          colorPalette={(failedTaskInstances?.total_entries ?? 0) === 0 ? "green" : "red"}
          count={failedTaskInstances?.total_entries ?? 0}
          endDate={endDate}
          events={(failedTaskInstances?.task_instances ?? []).map((ti) => ({
            timestamp: ti.start_date ?? ti.logical_date,
          }))}
          isLoading={isFailedTaskInstancesLoading}
          label={translate("overview.buttons.failedTaskInstance", {
            count: failedTaskInstances?.total_entries ?? 0,
          })}
          route={{
            pathname: "task_instances",
            search: `${SearchParamsKeys.TASK_STATE}=failed`,
          }}
          startDate={startDate}
        />
      </HStack>
      <SimpleGrid columns={3} gap={5} my={5}>
        <Box borderRadius={4} borderStyle="solid" borderWidth={1} p={2} width="350px">
          {isLoadingTaskInstances ? (
            <Skeleton height="200px" w="full" />
          ) : (
            <DurationChart entries={tiData?.task_instances.slice().reverse()} kind="Task Instance" />
          )}
        </Box>
      </SimpleGrid>
    </Box>
  );
};
