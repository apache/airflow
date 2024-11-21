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
import { Box, HStack } from "@chakra-ui/react";
import dayjs from "dayjs";
import { useState } from "react";
import { useParams } from "react-router-dom";

import {
  useDagRunServiceGetDagRuns,
  useTaskInstanceServiceGetTaskInstances,
} from "openapi/queries";
import TimeRangeSelector from "src/components/TimeRangeSelector";
import { TrendCountButton } from "src/components/TrendCountButton";
import { stateColor } from "src/utils/stateColor";

const defaultHour = "12";

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

  const { data: failedRuns, isLoading: isLoadingRuns } =
    useDagRunServiceGetDagRuns({
      dagId: dagId ?? "",
      logicalDateGte: startDate,
      logicalDateLte: endDate,
      state: ["failed"],
    });

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
      <HStack>
        <TrendCountButton
          colorPalette={stateColor.failed}
          count={failedTasks?.total_entries ?? 0}
          endDate={endDate}
          events={(failedTasks?.task_instances ?? []).map((ti) => ({
            timestamp: ti.start_date ?? ti.logical_date,
          }))}
          isLoading={isLoading}
          label="Failed Task"
          route={`${location.pathname}/tasks`}
          startDate={startDate}
        />
        <TrendCountButton
          colorPalette={stateColor.failed}
          count={failedRuns?.total_entries ?? 0}
          endDate={endDate}
          events={(failedRuns?.dag_runs ?? []).map((dr) => ({
            timestamp: dr.start_date ?? dr.logical_date ?? "",
          }))}
          isLoading={isLoadingRuns}
          label="Failed Run"
          route={{
            pathname: "runs",
            search: "state=failed",
          }}
          startDate={startDate}
        />
      </HStack>
    </Box>
  );
};
