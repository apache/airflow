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
import { Heading } from "@chakra-ui/react";
import { ReactFlowProvider } from "@xyflow/react";
import { useTranslation } from "react-i18next";
import { FiCode, FiDatabase, FiUser } from "react-icons/fi";
import {
  MdDetails,
  MdOutlineEventNote,
  MdOutlineStorage,
  MdOutlineTask,
  MdReorder,
  MdSyncAlt,
} from "react-icons/md";
import { PiBracketsCurlyBold } from "react-icons/pi";
import { Navigate, useLocation, useParams, useSearchParams } from "react-router-dom";

import { DetailsLayout } from "src/layouts/Details/DetailsLayout";
import type { NavTab } from "src/layouts/Details/NavTabs";

import { SearchParamsKeys } from "src/constants/searchParams";
import { useHITLReviewTabs } from "src/hooks/useHITLReviewTabs";
import { usePluginTabs } from "src/hooks/usePluginTabs";
import { type TabItem, useRequiredActionTabs } from "src/hooks/useRequiredActionTabs";
import { useTaskInstanceCoordinates } from "src/hooks/useTaskInstanceCoordinates";
import { useTaskInstanceView } from "src/hooks/useTaskInstanceView";
import { useDefaultTaskInstanceTab } from "src/hooks/useUserSettings";
import { useGridTiSummariesStream } from "src/queries/useGridTISummaries.ts";
import { isStatePending, useAutoRefresh, useDocumentTitle } from "src/utils";
import { getDefaultTaskInstanceTabPath, getTaskInstanceLink } from "src/utils/links";
import { isLoopTaskInstance } from "src/utils/loopTaskInstance";

import { Header, HistoryHeader } from "./Header";

export const TaskInstance = () => {
  const { t: translate } = useTranslation(["dag", "common", "hitl"]);
  const { dagId = "", mapIndex = "-1", runId = "", taskId = "" } = useParams();
  const [searchParams] = useSearchParams();
  const location = useLocation();
  const coordinates = useTaskInstanceCoordinates();
  const { error, historical, historicalTaskInstance, isLoading, liveTaskInstance, taskInstance } =
    useTaskInstanceView();
  const regional = liveTaskInstance !== undefined && isLoopTaskInstance(liveTaskInstance);
  const tryNumber = searchParams.get(SearchParamsKeys.TRY_NUMBER);
  const coordinateSearch = new URLSearchParams();

  if (coordinates.regionId !== undefined) {
    coordinateSearch.set(SearchParamsKeys.REGION_ID, coordinates.regionId);
  }
  if (coordinates.regionIndex !== undefined) {
    coordinateSearch.set(SearchParamsKeys.REGION_INDEX, String(coordinates.regionIndex));
  }
  const tryParams = new URLSearchParams(coordinateSearch);

  if (tryNumber !== null) {
    tryParams.set(SearchParamsKeys.TRY_NUMBER, tryNumber);
  }
  const trySearch = tryParams.toString() || undefined;

  useDocumentTitle(taskId);

  // Get external views with task_instance destination
  const externalTabs = usePluginTabs("task_instance");

  // When another tab is the default, the index route redirects to it, so the Logs
  // tab must point at the explicit /logs path to stay reachable.
  const [defaultTab] = useDefaultTaskInstanceTab();
  const logsTabValue = getDefaultTaskInstanceTabPath(defaultTab) === "" ? "" : "logs";

  const tabs: Array<NavTab & TabItem> = [
    {
      icon: <MdReorder />,
      label: translate("tabs.logs"),
      matchPaths: ["logs"],
      search: trySearch,
      value: logsTabValue,
    },
    {
      icon: <FiUser />,
      label: translate("tabs.requiredActions"),
      search: trySearch,
      value: "required_actions",
    },
    {
      icon: <PiBracketsCurlyBold />,
      label: translate("tabs.renderedTemplates"),
      value: "rendered_templates",
    },
    { icon: <MdSyncAlt />, label: translate("tabs.xcom"), value: "xcom" },
    { icon: <MdOutlineStorage />, label: translate("tabs.taskStateStore"), value: "task-state-store" },
    { icon: <FiDatabase />, label: translate("tabs.assetEvents"), value: "asset_events" },
    { icon: <MdOutlineEventNote />, label: translate("tabs.auditLog"), value: "events" },
    { icon: <FiCode />, label: translate("tabs.code"), value: "code" },
    { icon: <MdDetails />, label: translate("tabs.details"), search: trySearch, value: "details" },
    ...externalTabs,
  ];

  const refetchInterval = useAutoRefresh({ dagId });
  const parsedMapIndex = parseInt(mapIndex, 10);

  const { summariesByRunId } = useGridTiSummariesStream({
    dagId,
    runIds: !historical && runId ? [runId] : [],
  });
  const gridTISummaries = summariesByRunId.get(runId);

  const taskInstanceSummary = gridTISummaries?.task_instances.find((ti) => ti.task_id === taskId);
  const taskCount = Object.entries(taskInstanceSummary?.child_states ?? {})
    .map(([_state, count]) => count)
    .reduce((sum, val) => sum + val, 0);
  const scopedTabs = tabs
    .filter((tab) => !historical || ["details", logsTabValue].includes(tab.value))
    .map((tab) => ({
      ...tab,
      search: tab.search ?? (coordinateSearch.toString() || undefined),
    }));
  const newTabs =
    taskInstance && taskInstance.map_index > -1 && !regional && !historical
      ? [
          ...scopedTabs.slice(0, 1),
          {
            icon: <MdOutlineTask />,
            label: translate("tabs.mappedTaskInstances_other", {
              count: Number(taskCount),
            }),
            value: "task_instances",
          },
          ...scopedTabs.slice(1),
        ]
      : scopedTabs;

  const { tabs: requiredActionTabs } = useRequiredActionTabs(
    { ...coordinates, dagId, dagRunId: runId, mapIndex: parsedMapIndex, taskId },
    newTabs,
    {
      autoRedirect: true,
      enabled: !historical,
      refetchInterval: isStatePending(taskInstance?.state) && refetchInterval,
    },
  );

  const { tabs: displayTabs } = useHITLReviewTabs({ dagId, dagRunId: runId, taskId }, requiredActionTabs, {
    ...coordinates,
    enabled: !historical,
    mapIndex: parsedMapIndex,
    refetchInterval: isStatePending(taskInstance?.state) && refetchInterval,
  });

  const taskPath = getTaskInstanceLink({ dagId, dagRunId: runId, mapIndex: parsedMapIndex, taskId });

  if (historical && ![`${taskPath}/details`, `${taskPath}/logs`, taskPath].includes(location.pathname)) {
    return <Navigate replace to={{ pathname: `${taskPath}/logs`, search: searchParams.toString() }} />;
  }

  return (
    <ReactFlowProvider>
      <DetailsLayout error={error} isLoading={isLoading} tabs={displayTabs}>
        {taskInstance === undefined ? (
          isLoading ? undefined : (
            <Heading p={2} size="lg">
              {translate("common:noItemsFound", { modelName: translate("common:taskInstance_one") })}
            </Heading>
          )
        ) : historicalTaskInstance === undefined ? (
          liveTaskInstance === undefined ? undefined : (
            <Header taskInstance={liveTaskInstance} />
          )
        ) : (
          <HistoryHeader taskInstance={historicalTaskInstance} />
        )}
      </DetailsLayout>
    </ReactFlowProvider>
  );
};
