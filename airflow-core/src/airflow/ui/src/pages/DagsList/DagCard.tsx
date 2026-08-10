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
import { Box, Flex, Grid, GridItem, HStack, Spinner, Text } from "@chakra-ui/react";
import { useTranslation } from "react-i18next";

import type { DAGWithLatestDagRunsResponse } from "openapi/requests/types.gen";
import DagRunInfo from "src/components/DagRunInfo";
import { Stat } from "src/components/Stat";
import { RouterLink, Tooltip } from "src/components/ui";
import { useNearViewport } from "src/hooks/useNearViewport";
import { useConfig } from "src/queries/useConfig";
import { isStatePending, useAutoRefresh } from "src/utils";

import { DagCardActions } from "./DagCardActions";
import { DagRunStateCounts } from "./DagRunStateCounts";
import { DagTags } from "./DagTags";
import { RecentRuns } from "./RecentRuns";
import { Schedule } from "./Schedule";

type Props = {
  readonly dag: DAGWithLatestDagRunsResponse;
  readonly runStateCounts: Record<string, number> | undefined;
  readonly runStateCountsLoading: boolean;
  readonly stateCountLimit: number | undefined;
};

export const DagCard = ({ dag, runStateCounts, runStateCountsLoading, stateCountLimit }: Props) => {
  const { t: translate } = useTranslation(["common", "dag"]);
  const [latestRun] = dag.latest_dag_runs;
  const multiTeamEnabled = Boolean(useConfig("multi_team"));
  const { isNearViewport, ref, showContent } = useNearViewport<HTMLDivElement>();

  const refetchInterval = useAutoRefresh({});

  return (
    <Box
      borderColor="border.emphasized"
      borderRadius={8}
      borderWidth={1}
      data-testid="dag-card"
      minHeight="140px"
      onFocusCapture={showContent}
      overflow="hidden"
      ref={ref}
    >
      <Flex alignItems="center" bg="bg.muted" justifyContent="space-between" px={3} py={1}>
        <HStack>
          <Tooltip content={dag.description} disabled={!Boolean(dag.description)}>
            <RouterLink color="fg.info" data-testid="dag-id" fontWeight="bold" to={`/dags/${dag.dag_id}`}>
              {dag.dag_display_name}
            </RouterLink>
          </Tooltip>
          <DagTags tags={dag.tags} />
        </HStack>
        <HStack data-testid="dag-card-actions" gap={1} minHeight="32px">
          {isNearViewport ? <DagCardActions dag={dag} /> : undefined}
        </HStack>
      </Flex>
      <Grid
        gap={1}
        px={3}
        py={2}
        templateColumns={multiTeamEnabled ? "repeat(5, 1fr)" : "repeat(4, 1fr)"}
        templateRows="auto auto"
      >
        <GridItem gridColumn={1} gridRow={1}>
          <Stat data-testid="schedule" label={translate("dagDetails.schedule")}>
            {isNearViewport ? (
              <Schedule
                assetExpression={dag.asset_expression}
                dagId={dag.dag_id}
                timetableDescription={dag.timetable_description}
                timetablePartitioned={dag.timetable_partitioned}
                timetableSummary={dag.timetable_summary}
              />
            ) : (
              <Text fontSize="sm">{dag.timetable_summary}</Text>
            )}
          </Stat>
        </GridItem>
        <GridItem gridColumn={2} gridRow={1}>
          <Stat data-testid="latest-run" label={translate("dagDetails.latestRun")}>
            {latestRun ? (
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
            ) : undefined}
          </Stat>
        </GridItem>
        <GridItem gridColumn={3} gridRow={1}>
          <Stat data-testid="next-run" label={translate("dagDetails.nextRun")}>
            {!dag.is_paused && Boolean(dag.next_dagrun_run_after) ? (
              <DagRunInfo
                logicalDate={dag.next_dagrun_logical_date}
                runAfter={dag.next_dagrun_run_after as string}
              />
            ) : undefined}
          </Stat>
        </GridItem>
        {multiTeamEnabled ? (
          <GridItem gridColumn={4} gridRow={1}>
            <Stat label={translate("dagDetails.team")}>
              {dag.team_name === undefined || dag.team_name === null ? undefined : (
                <RouterLink to={`/dags?teams=${encodeURIComponent(dag.team_name)}`}>
                  {dag.team_name}
                </RouterLink>
              )}
            </Stat>
          </GridItem>
        ) : undefined}
        <GridItem
          alignItems="flex-end"
          display="flex"
          gridColumn={multiTeamEnabled ? 5 : 4}
          gridRow="1 / 3"
          justifyContent="flex-end"
        >
          <Box minHeight="65px">
            {isNearViewport ? <RecentRuns latestRuns={dag.latest_dag_runs} /> : undefined}
          </Box>
        </GridItem>
        <GridItem alignSelf="end" gridColumn={1} gridRow={2}>
          <Box minHeight="22px">
            {isNearViewport ? (
              <DagRunStateCounts
                counts={runStateCounts}
                dagId={dag.dag_id}
                isLoading={runStateCountsLoading}
                stateCountLimit={stateCountLimit}
              />
            ) : undefined}
          </Box>
        </GridItem>
      </Grid>
    </Box>
  );
};
