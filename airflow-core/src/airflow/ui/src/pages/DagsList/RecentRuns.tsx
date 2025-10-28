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
import { useTranslation } from "react-i18next";
import { Link } from "react-router-dom";

import type { DAGWithLatestDagRunsResponse } from "openapi/requests/types.gen";
import { StateIcon } from "src/components/StateIcon";
import Time from "src/components/Time";
import { Tooltip } from "src/components/ui";
import { renderDuration } from "src/utils";

dayjs.extend(duration);

const BAR_HEIGHT = 65;

export const RecentRuns = ({
  latestRuns,
}: {
  readonly latestRuns: DAGWithLatestDagRunsResponse["latest_dag_runs"];
}) => {
  const { t: translate } = useTranslation();

  if (!latestRuns.length) {
    return undefined;
  }

  const max = Math.max.apply(
    undefined,
    latestRuns.map((run) => run.duration ?? 0),
  );

  return (
    <Flex alignItems="flex-end" flexDirection="row-reverse" gap={[0.5, 0.5, 0.5, 1]} pb={1}>
      {latestRuns.map((run) => (
        <Tooltip
          content={
            <Box>
              <Text>
                {translate("state")}: {translate(`common:states.${run.state}`)}
              </Text>
              <Text>
                {translate("dagRun.runAfter")}: <Time datetime={run.run_after} />
              </Text>
              {run.start_date === null ? undefined : (
                <Text>
                  {translate("startDate")}: <Time datetime={run.start_date} />
                </Text>
              )}
              {run.end_date === null ? undefined : (
                <Text>
                  {translate("endDate")}: <Time datetime={run.end_date} />
                </Text>
              )}
              <Text>
                {translate("duration")}: {renderDuration(run.duration)}
              </Text>
            </Box>
          }
          key={run.run_id}
          positioning={{
            offset: {
              crossAxis: 5,
              mainAxis: 5,
            },
            placement: "bottom-start",
          }}
        >
          <Link to={`/dags/${run.dag_id}/runs/${run.run_id}/`}>
            <Flex
              alignItems="center"
              bg={`${run.state}.solid`}
              borderRadius="4px"
              flexDir="column"
              fontSize="12px"
              height={`${run.duration === null ? 1 : (run.duration / max) * BAR_HEIGHT}px`}
              justifyContent="flex-end"
              minHeight="12px"
              width="12px"
            >
              <StateIcon color="white" state={run.state} />
            </Flex>
          </Link>
        </Tooltip>
      ))}
    </Flex>
  );
};
