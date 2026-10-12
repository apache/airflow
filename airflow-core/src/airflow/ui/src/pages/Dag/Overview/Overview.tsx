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
import { lazy, useState, Suspense } from "react";

import { Box, HStack, Skeleton, VStack } from "@chakra-ui/react";
import dayjs from "dayjs";
import { useTranslation } from "react-i18next";
import { FiBarChart } from "react-icons/fi";
import { useParams } from "react-router-dom";
import { useLocalStorage } from "usehooks-ts";

import {
  useAssetServiceGetAssetEvents,
  useDagRunServiceGetDagRuns,
  useDagServiceGetLatestRunInfo,
  useDagServiceGetRecentTaskInstanceStateCountsUi,
  usePluginServiceGetPlugins,
  useTaskInstanceServiceGetTaskInstances,
} from "openapi/queries";

import { ReactPlugin } from "src/pages/ReactPlugin";

import { AssetEvents } from "src/components/Assets/AssetEvents";
import { DurationChart } from "src/components/DurationChart";
import { LatestRunCard, LatestRunCardSkeleton } from "src/components/LatestRunCard";
import { RunTaskStateBadges } from "src/components/RunTaskStateBadges";
import { SlowestTaskInstancesChart } from "src/components/SlowestTaskInstancesChart";
import TimeRangeSelector from "src/components/TimeRangeSelector";
import { TrendCountButton } from "src/components/TrendCountButton";

import { dagRunsLimitKey } from "src/constants/localStorage";
import { SearchParamsKeys } from "src/constants/searchParams";
import { useScopedPluginViews } from "src/hooks/useScopedPluginViews";
import { useGridRuns } from "src/queries/useGridRuns.ts";
import { isStatePending, useAutoRefresh } from "src/utils";

import { DagDeadlines } from "./DagDeadlines";

const FailedLogs = lazy(() => import("./FailedLogs"));

const defaultHour = "24";

export const Overview = () => {
  const { t: translate } = useTranslation("dag");
  const { dagId } = useParams();

  const now = dayjs();
  const [startDate, setStartDate] = useState(now.subtract(Number(defaultHour), "hour").toISOString());
  const [endDate, setEndDate] = useState(now.toISOString());
  const [assetSortBy, setAssetSortBy] = useState("-timestamp");

  const { data: failedTasks } = useTaskInstanceServiceGetTaskInstances({
    dagId: dagId ?? "",
    dagRunId: "~",
    orderBy: ["-run_after"],
    runAfterGte: startDate,
    runAfterLte: endDate,
    state: ["failed"],
  });

  const failedTaskCount = failedTasks?.total_entries ?? 0;

  const { data: slowestTaskInstancesData, isLoading: isLoadingSlowestTaskInstances } =
    useTaskInstanceServiceGetTaskInstances(
      {
        dagId: dagId ?? "",
        dagRunId: "~",
        limit: 10,
        orderBy: ["-duration"],
        runAfterGte: startDate,
        runAfterLte: endDate,
        state: ["success", "failed"],
      },
      undefined,
      { enabled: Boolean(dagId) },
    );

  const [limit] = useLocalStorage<number>(dagRunsLimitKey(dagId ?? ""), 10);

  const { data: failedRuns } = useDagRunServiceGetDagRuns({
    dagId: dagId ?? "",
    limit,
    runAfterGte: startDate,
    runAfterLte: endDate,
    state: ["failed"],
  });
  const failedRunCount = failedRuns?.total_entries ?? 0;
  const { data: gridRuns, isLoading: isLoadingRuns } = useGridRuns({ limit });
  const refetchInterval = useAutoRefresh({ dagId });
  // Shares its cache with the Dag page header, which owns the polling for this query.
  const { data: latestRun, isLoading: isLoadingLatestRun } = useDagServiceGetLatestRunInfo(
    { dagId: dagId ?? "" },
    undefined,
    { enabled: Boolean(dagId) },
  );
  const { data: taskStateCounts, isLoading: isLoadingTaskStateCounts } =
    useDagServiceGetRecentTaskInstanceStateCountsUi(
      { dagRunIds: latestRun ? [latestRun.id] : [] },
      undefined,
      {
        enabled: Boolean(latestRun),
        refetchInterval: () => (latestRun && isStatePending(latestRun.state) ? refetchInterval : false),
      },
    );
  const isAutoRefreshing =
    Boolean(refetchInterval) && (gridRuns ?? []).some((run) => isStatePending(run.state));
  const { data: assetEventsData, isLoading: isLoadingAssetEvents } = useAssetServiceGetAssetEvents({
    limit,
    orderBy: [assetSortBy],
    sourceDagId: dagId,
    timestampGte: startDate,
    timestampLte: endDate,
  });
  const { data: pluginData } = usePluginServiceGetPlugins();
  const reactApps = pluginData?.plugins.flatMap((plugin) => plugin.react_apps) ?? [];
  const dagOverviewReactPlugins = useScopedPluginViews(reactApps, "dag_overview");

  return (
    <VStack alignItems="stretch" gap={4} m={4}>
      <Box my={2} order={1}>
        <TimeRangeSelector
          defaultValue={defaultHour}
          endDate={endDate}
          setEndDate={setEndDate}
          setStartDate={setStartDate}
          startDate={startDate}
        />
      </Box>
      <HStack alignItems="flex-start" flexWrap="wrap" order={2}>
        {isLoadingLatestRun ? (
          <LatestRunCardSkeleton />
        ) : latestRun === null || latestRun === undefined ? undefined : (
          <LatestRunCard
            icon={<FiBarChart />}
            label={translate("overview.latestRun.dagRun")}
            state={latestRun.state}
            taskStates={
              <RunTaskStateBadges
                isLoading={isLoadingTaskStateCounts}
                stateCounts={taskStateCounts?.dags[0]?.state_counts}
              />
            }
            title={latestRun.run_id}
            to={`/dags/${latestRun.dag_id}/runs/${latestRun.run_id}`}
          />
        )}
        {failedTaskCount > 0 ? (
          <TrendCountButton
            colorPalette="failed"
            count={failedTaskCount}
            endDate={endDate}
            events={(failedTasks?.task_instances ?? []).map((ti) => ({
              timestamp: ti.start_date ?? ti.logical_date,
            }))}
            label={translate("overview.buttons.failedTask", { count: failedTaskCount })}
            route={{
              pathname: "tasks",
              search: `${SearchParamsKeys.STATE}=failed`,
            }}
            startDate={startDate}
          />
        ) : undefined}
        {failedRunCount > 0 ? (
          <TrendCountButton
            colorPalette="failed"
            count={failedRunCount}
            endDate={endDate}
            events={(failedRuns?.dag_runs ?? []).map((dr) => ({
              timestamp: dr.run_after,
            }))}
            label={translate("overview.buttons.failedRun", { count: failedRunCount })}
            route={{
              pathname: "runs",
              search: `${SearchParamsKeys.STATE}=failed`,
            }}
            startDate={startDate}
          />
        ) : undefined}
      </HStack>
      <HStack alignItems="flex-start" flexWrap="wrap" order={3}>
        <Box
          borderRadius={4}
          borderStyle="solid"
          borderWidth={1}
          flex="1 1 520px"
          maxWidth="900px"
          minWidth="320px"
          p={2}
        >
          {isLoadingRuns ? (
            <Skeleton height="310px" w="full" />
          ) : (
            <DurationChart
              entries={gridRuns?.slice().reverse()}
              isAutoRefreshing={isAutoRefreshing}
              kind="Dag Run"
            />
          )}
        </Box>
        <Box
          borderRadius={4}
          borderStyle="solid"
          borderWidth={1}
          flex="1 1 520px"
          maxWidth="900px"
          minWidth="320px"
          p={2}
        >
          {isLoadingSlowestTaskInstances ? (
            <Skeleton height="380px" w="full" />
          ) : (
            <SlowestTaskInstancesChart taskInstances={slowestTaskInstancesData?.task_instances ?? []} />
          )}
        </Box>
        {assetEventsData && assetEventsData.total_entries > 0 ? (
          <AssetEvents
            data={assetEventsData}
            isLoading={isLoadingAssetEvents}
            ml={0}
            setOrderBy={setAssetSortBy}
            titleKey="dag:overview.charts.assetEvent"
          />
        ) : undefined}
      </HStack>
      {dagId === undefined ? undefined : (
        <Box css={{ "&:empty": { display: "none" } }} order={4}>
          <DagDeadlines dagId={dagId} />
        </Box>
      )}
      <Box css={{ "&:empty": { display: "none" } }} order={5}>
        <Suspense fallback={<Skeleton height="100px" width="full" />}>
          <FailedLogs failedTasks={failedTasks} />
        </Suspense>
      </Box>
      {dagOverviewReactPlugins.map((plugin) => (
        <ReactPlugin key={plugin.name} reactApp={plugin} />
      ))}
    </VStack>
  );
};
