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
import { useMemo } from "react";
import { useTranslation } from "react-i18next";
import { FiCode, FiDatabase, FiUser } from "react-icons/fi";
import { MdDetails, MdOutlineEventNote, MdOutlineTask, MdReorder, MdSyncAlt } from "react-icons/md";
import { PiBracketsCurlyBold } from "react-icons/pi";
import { useParams } from "react-router-dom";

import { useTaskInstanceServiceGetMappedTaskInstance } from "openapi/queries";
import { usePluginTabs } from "src/hooks/usePluginTabs";
import { useRequiredActionTabs } from "src/hooks/useRequiredActionTabs";
import { DetailsLayout } from "src/layouts/Details/DetailsLayout";
import { useGridTiSummaries } from "src/queries/useGridTISummaries.ts";
import { isStatePending, useAutoRefresh } from "src/utils";

import { Header } from "./Header";

export const TaskInstance = () => {
  const { t: translate } = useTranslation(["dag", "common", "hitl"]);
  const { dagId = "", mapIndex = "-1", runId = "", taskId = "" } = useParams();
  // Get external views with task_instance destination
  const externalTabs = usePluginTabs("task_instance");

  const tabs = [
    { icon: <MdReorder />, label: translate("tabs.logs"), value: "" },
    { icon: <FiUser />, label: translate("tabs.requiredActions"), value: "required_actions" },
    {
      icon: <PiBracketsCurlyBold />,
      label: translate("tabs.renderedTemplates"),
      value: "rendered_templates",
    },
    { icon: <MdSyncAlt />, label: translate("tabs.xcom"), value: "xcom" },
    { icon: <FiDatabase />, label: translate("tabs.assetEvents"), value: "asset_events" },
    { icon: <MdOutlineEventNote />, label: translate("tabs.auditLog"), value: "events" },
    { icon: <FiCode />, label: translate("tabs.code"), value: "code" },
    { icon: <MdDetails />, label: translate("tabs.details"), value: "details" },
    ...externalTabs,
  ];

  const refetchInterval = useAutoRefresh({ dagId });
  const parsedMapIndex = parseInt(mapIndex, 10);

  const {
    data: taskInstance,
    error,
    isLoading,
  } = useTaskInstanceServiceGetMappedTaskInstance(
    {
      dagId,
      dagRunId: runId,
      mapIndex: parsedMapIndex,
      taskId,
    },
    undefined,
    {
      enabled: !isNaN(parsedMapIndex),
      refetchInterval: (query) => (isStatePending(query.state.data?.state) ? refetchInterval : false),
    },
  );

  const { data: gridTISummaries } = useGridTiSummaries({ dagId, runId });

  const taskInstanceSummary = gridTISummaries?.task_instances.find((ti) => ti.task_id === taskId);
  const taskCount = useMemo(
    () =>
      Object.entries(taskInstanceSummary?.child_states ?? {})
        .map(([_state, count]) => count)
        .reduce((sum, val) => sum + val, 0),
    [taskInstanceSummary],
  );
  let newTabs = tabs;

  if (taskInstance && taskInstance.map_index > -1) {
    newTabs = [
      ...tabs.slice(0, 1),
      {
        icon: <MdOutlineTask />,
        label: translate("tabs.mappedTaskInstances_other", {
          count: Number(taskCount),
        }),
        value: "task_instances",
      },
      ...tabs.slice(1),
    ];
  }

  const { tabs: displayTabs } = useRequiredActionTabs({ dagId, dagRunId: runId, taskId }, newTabs, {
    autoRedirect: true,
    refetchInterval: isStatePending(taskInstance?.state) ? refetchInterval : false,
  });

  return (
    <ReactFlowProvider>
      <DetailsLayout error={error} isLoading={isLoading} tabs={displayTabs}>
        {taskInstance === undefined ? (
          <Heading p={2} size="lg">
            {translate("common:noItemsFound", { modelName: translate("common:taskInstance_one") })}
          </Heading>
        ) : (
          <Header taskInstance={taskInstance} />
        )}
      </DetailsLayout>
    </ReactFlowProvider>
  );
};
