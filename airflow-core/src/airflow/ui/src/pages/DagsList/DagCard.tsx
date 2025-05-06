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
import { Box, Flex, HStack, SimpleGrid, Link, Spinner } from "@chakra-ui/react";
import { Link as RouterLink } from "react-router-dom";

import type { DAGWithLatestDagRunsResponse } from "openapi/requests/types.gen";
import DagRunInfo from "src/components/DagRunInfo";
import { Stat } from "src/components/Stat";
import { TogglePause } from "src/components/TogglePause";
import TriggerDAGButton from "src/components/TriggerDag/TriggerDAGButton";
import { Tooltip } from "src/components/ui";
import { isStatePending, useAutoRefresh } from "src/utils";

import { DagTags } from "./DagTags";
import { RecentRuns } from "./RecentRuns";
import { Schedule } from "./Schedule";

type Props = {
  readonly dag: DAGWithLatestDagRunsResponse;
};

export const DagCard = ({ dag }: Props) => {
  const [latestRun] = dag.latest_dag_runs;

  const refetchInterval = useAutoRefresh({ isPaused: dag.is_paused });

  return (
    <Box borderColor="border.emphasized" borderRadius={8} borderWidth={1} overflow="hidden">
      <Flex alignItems="center" bg="bg.muted" justifyContent="space-between" px={3} py={1}>
        <HStack>
          <Tooltip content={dag.description} disabled={!Boolean(dag.description)}>
            <Link asChild color="fg.info" fontWeight="bold">
              <RouterLink to={`/dags/${dag.dag_id}`}>{dag.dag_display_name}</RouterLink>
            </Link>
          </Tooltip>
          <DagTags tags={dag.tags} />
        </HStack>
        <HStack>
          <TogglePause dagDisplayName={dag.dag_display_name} dagId={dag.dag_id} isPaused={dag.is_paused} />
          <TriggerDAGButton dag={dag} withText={false} />
        </HStack>
      </Flex>
      <SimpleGrid columns={4} gap={1} height={20} px={3} py={1}>
        <Stat label="Schedule">
          <Schedule dag={dag} />
        </Stat>
        <Stat label="Latest Run">
          {latestRun ? (
            <Link asChild color="fg.info">
              <RouterLink to={`/dags/${latestRun.dag_id}/runs/${latestRun.dag_run_id}`}>
                <DagRunInfo
                  endDate={latestRun.end_date}
                  logicalDate={latestRun.logical_date}
                  runAfter={latestRun.run_after}
                  startDate={latestRun.start_date}
                  state={latestRun.state}
                />
                {isStatePending(latestRun.state) && Boolean(refetchInterval) ? <Spinner /> : undefined}
              </RouterLink>
            </Link>
          ) : undefined}
        </Stat>
        <Stat label="Next Run">
          {Boolean(dag.next_dagrun_run_after) ? (
            <DagRunInfo
              logicalDate={dag.next_dagrun_logical_date}
              runAfter={dag.next_dagrun_run_after as string}
            />
          ) : undefined}
        </Stat>
        <RecentRuns latestRuns={dag.latest_dag_runs} />
      </SimpleGrid>
    </Box>
  );
};
