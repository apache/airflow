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
import { useParams } from "react-router-dom";

import { useDagRunServiceGetDagRuns, useTaskInstanceServiceGetTaskInstances } from "openapi/queries";
import { DurationChart } from "src/components/DurationChart";
import TimeRangeSelector from "src/components/TimeRangeSelector";
import { TrendCountButton } from "src/components/TrendCountButton";

const defaultHour = "168";

export const Overview = () => {
  const { dagId } = useParams();

  const now = dayjs();
  const [startDate, setStartDate] = useState(now.subtract(Number(defaultHour), "hour").toISOString());
  const [endDate, setEndDate] = useState(now.toISOString());

  const { data: failedTasks, isLoading } = useTaskInstanceServiceGetTaskInstances({
    dagId: dagId ?? "",
    dagRunId: "~",
    runAfterGte: startDate,
    runAfterLte: endDate,
    state: ["failed"],
  });

  const { data: failedRuns, isLoading: isLoadingFailedRuns } = useDagRunServiceGetDagRuns({
    dagId: dagId ?? "",
    runAfterGte: startDate,
    runAfterLte: endDate,
    state: ["failed"],
  });

  const { data: runs, isLoading: isLoadingRuns } = useDagRunServiceGetDagRuns({
    dagId: dagId ?? "",
    limit: 14,
    orderBy: "-run_after",
  });

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
      <HStack>
        <TrendCountButton
          colorPalette="failed"
          count={failedTasks?.total_entries ?? 0}
          endDate={endDate}
          events={(failedTasks?.task_instances ?? []).map((ti) => ({
            timestamp: ti.start_date ?? ti.logical_date,
          }))}
          isLoading={isLoading}
          label="Failed Task"
          route={{
            pathname: "tasks",
            search: "state=failed",
          }}
          startDate={startDate}
        />
        <TrendCountButton
          colorPalette="failed"
          count={failedRuns?.total_entries ?? 0}
          endDate={endDate}
          events={(failedRuns?.dag_runs ?? []).map((dr) => ({
            timestamp: dr.run_after,
          }))}
          isLoading={isLoadingFailedRuns}
          label="Failed Run"
          route={{
            pathname: "runs",
            search: "state=failed",
          }}
          startDate={startDate}
        />
      </HStack>
      <SimpleGrid columns={3} gap={5} my={5}>
        <Box borderRadius={4} borderStyle="solid" borderWidth={1} p={2}>
          {isLoadingRuns ? (
            <Skeleton height="200px" w="full" />
          ) : (
            <DurationChart entries={runs?.dag_runs.slice().reverse()} kind="Dag Run" />
          )}
        </Box>
      </SimpleGrid>
    </Box>
  );
};
