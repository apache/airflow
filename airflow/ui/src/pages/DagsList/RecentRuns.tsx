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
import { Flex, Box, Tooltip, Text } from "@chakra-ui/react";
import dayjs from "dayjs";
import duration from "dayjs/plugin/duration";

import type { DAGWithLatestDagRunsResponse } from "openapi/requests/types.gen";
import Time from "src/components/Time";
import { stateColor } from "src/utils/stateColor";

dayjs.extend(duration);

const BAR_HEIGHT = 60;

export const RecentRuns = ({
  latestRuns,
}: {
  readonly latestRuns: DAGWithLatestDagRunsResponse["latest_dag_runs"];
}) => {
  if (!latestRuns.length) {
    return undefined;
  }

  const runsWithDuration = latestRuns.map((run) => ({
    ...run,
    duration: dayjs
      .duration(dayjs(run.end_date).diff(run.start_date))
      .asSeconds(),
  }));

  const max = Math.max.apply(
    undefined,
    runsWithDuration.map((run) => run.duration),
  );

  return (
    <Flex alignItems="flex-end" flexDirection="row-reverse">
      {runsWithDuration.map((run) => (
        <Tooltip
          hasArrow
          key={run.run_id}
          label={
            <Box>
              <Text>State: {run.state}</Text>
              <Text>
                Logical Date: <Time datetime={run.logical_date} />
              </Text>
              <Text>Duration: {run.duration.toFixed(2)}s</Text>
            </Box>
          }
          offset={[10, 5]}
          placement="bottom-start"
        >
          <Box p={1}>
            <Box
              bg={stateColor[run.state]}
              borderRadius="4px"
              height={`${(run.duration / max) * BAR_HEIGHT}px`}
              minHeight={1}
              width="4px"
            />
          </Box>
        </Tooltip>
      ))}
    </Flex>
  );
};
