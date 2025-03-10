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
import { Flex, Box, Text } from "@chakra-ui/react";
import dayjs from "dayjs";
import duration from "dayjs/plugin/duration";
import { Link } from "react-router-dom";

import type { DAGWithLatestDagRunsResponse } from "openapi/requests/types.gen";
import Time from "src/components/Time";
import { Tooltip } from "src/components/ui";

dayjs.extend(duration);

const BAR_HEIGHT = 65;

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
    duration: dayjs.duration(dayjs(run.end_date).diff(run.start_date)).asSeconds(),
  }));

  const max = Math.max.apply(
    undefined,
    runsWithDuration.map((run) => run.duration),
  );

  return (
    <Flex alignItems="flex-end" flexDirection="row-reverse" pb={1}>
      {runsWithDuration.map((run) => (
        <Tooltip
          content={
            <Box>
              <Text>State: {run.state}</Text>
              <Text>
                Run After: <Time datetime={run.run_after} />
              </Text>
              {run.start_date === null ? undefined : (
                <Text>
                  Start Date: <Time datetime={run.start_date} />
                </Text>
              )}
              {run.end_date === null ? undefined : (
                <Text>
                  End Date: <Time datetime={run.end_date} />
                </Text>
              )}
              <Text>Duration: {run.duration.toFixed(2)}s</Text>
            </Box>
          }
          key={run.dag_run_id}
          positioning={{
            offset: {
              crossAxis: 5,
              mainAxis: 5,
            },
            placement: "bottom-start",
          }}
        >
          <Link to={`/dags/${run.dag_id}/runs/${run.dag_run_id}/`}>
            <Box px={1}>
              <Box
                bg={`${run.state}.solid`}
                borderRadius="4px"
                height={`${(run.duration / max) * BAR_HEIGHT}px`}
                minHeight={1}
                width="4px"
              />
            </Box>
          </Link>
        </Tooltip>
      ))}
    </Flex>
  );
};
