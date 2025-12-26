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
import { useTranslation } from "react-i18next";
import { Link as RouterLink } from "react-router-dom";

import type { DAGWithLatestDagRunsResponse } from "openapi/requests/types.gen";
import DeleteDagButton from "src/components/DagActions/DeleteDagButton";
import { FavoriteDagButton } from "src/components/DagActions/FavoriteDagButton";
import DagRunInfo from "src/components/DagRunInfo";
import { NeedsReviewBadge } from "src/components/NeedsReviewBadge";
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
  const { t: translate } = useTranslation(["common", "dag"]);
  const [latestRun] = dag.latest_dag_runs;

  const refetchInterval = useAutoRefresh({});

  return (
    <Box borderColor="border.emphasized" borderRadius={8} borderWidth={1} overflow="hidden">
      <Flex alignItems="center" bg="bg.muted" justifyContent="space-between" px={3} py={1}>
        <HStack>
          <Tooltip content={dag.description} disabled={!Boolean(dag.description)}>
            <Link asChild color="fg.info" fontWeight="bold">
              <RouterLink data-testid="dag-id" to={`/dags/${dag.dag_id}`}>
                {dag.dag_display_name}
              </RouterLink>
            </Link>
          </Tooltip>
          <DagTags tags={dag.tags} />
        </HStack>
        <HStack>
          <NeedsReviewBadge dagId={dag.dag_id} pendingActions={dag.pending_actions} />
          <TogglePause
            dagDisplayName={dag.dag_display_name}
            dagId={dag.dag_id}
            isPaused={dag.is_paused}
            pr={2}
          />
          <TriggerDAGButton
            dagDisplayName={dag.dag_display_name}
            dagId={dag.dag_id}
            isPaused={dag.is_paused}
            withText={false}
          />
          <FavoriteDagButton dagId={dag.dag_id} isFavorite={dag.is_favorite} withText={false} />
          <DeleteDagButton dagDisplayName={dag.dag_display_name} dagId={dag.dag_id} withText={false} />
        </HStack>
      </Flex>
      <SimpleGrid columns={4} gap={1} height={20} px={3} py={1}>
        <Stat data-testid="schedule" label={translate("dagDetails.schedule")}>
          <Schedule
            assetExpression={dag.asset_expression}
            dagId={dag.dag_id}
            latestRunAfter={latestRun?.run_after}
            timetableDescription={dag.timetable_description}
            timetableSummary={dag.timetable_summary}
          />
        </Stat>
        <Stat data-testid="latest-run" label={translate("dagDetails.latestRun")}>
          {latestRun ? (
            <Link asChild color="fg.info">
              <RouterLink to={`/dags/${latestRun.dag_id}/runs/${latestRun.run_id}`}>
                <DagRunInfo
                  endDate={latestRun.end_date}
                  logicalDate={latestRun.logical_date}
                  runAfter={latestRun.run_after}
                  startDate={latestRun.start_date}
                  state={latestRun.state}
                />
                {isStatePending(latestRun.state) && !dag.is_paused && Boolean(refetchInterval) ? (
                  <Spinner />
                ) : undefined}
              </RouterLink>
            </Link>
          ) : undefined}
        </Stat>
        <Stat data-testid="next-run" label={translate("dagDetails.nextRun")}>
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
