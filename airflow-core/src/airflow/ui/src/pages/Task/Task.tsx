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
import { ReactFlowProvider } from "@xyflow/react";
import { useTranslation } from "react-i18next";
import { FiUser } from "react-icons/fi";
import { LuChartColumn } from "react-icons/lu";
import { MdOutlineEventNote, MdOutlineTask } from "react-icons/md";
import { useParams } from "react-router-dom";

import { useTaskServiceGetTask } from "openapi/queries";
import { usePluginTabs } from "src/hooks/usePluginTabs";
import { useRequiredActionTabs } from "src/hooks/useRequiredActionTabs";
import { DetailsLayout } from "src/layouts/Details/DetailsLayout";
import { useGridStructure } from "src/queries/useGridStructure.ts";
import { getGroupTask } from "src/utils/groupTask";

import { GroupTaskHeader } from "./GroupTaskHeader";
import { Header } from "./Header";

export const Task = () => {
  const { t: translate } = useTranslation(["dag", "hitl"]);
  const { dagId = "", groupId, runId, taskId } = useParams();

  // Get external views with task destination
  const externalTabs = usePluginTabs("task");

  const tabs = [
    { icon: <LuChartColumn />, label: translate("tabs.overview"), value: "" },
    { icon: <MdOutlineTask />, label: translate("tabs.taskInstances"), value: "task_instances" },
    { icon: <FiUser />, label: translate("tabs.requiredActions"), value: "required_actions" },
    { icon: <MdOutlineEventNote />, label: translate("tabs.auditLog"), value: "events" },
    ...externalTabs,
  ];

  const {
    data: task,
    error,
    isLoading,
  } = useTaskServiceGetTask({ dagId, taskId: groupId ?? taskId }, undefined, {
    enabled: groupId === undefined,
  });

  const { data: dagStructure } = useGridStructure({ limit: 1 });

  const groupTask = getGroupTask(dagStructure, groupId);

  // Handle required action tabs with shared utility
  const { tabs: processedTabs } = useRequiredActionTabs(
    {
      dagId,
      dagRunId: runId,
      taskId: Boolean(groupId) ? undefined : taskId,
      taskIdPattern: groupId,
    },
    tabs,
  );

  // Filter out events tab for group tasks
  const displayTabs =
    groupId === undefined ? processedTabs : processedTabs.filter((tab) => tab.value !== "events");

  return (
    <ReactFlowProvider>
      <DetailsLayout error={error} isLoading={isLoading} tabs={displayTabs}>
        {task === undefined ? undefined : <Header task={task} />}
        {groupTask ? <GroupTaskHeader title={groupTask.label} /> : undefined}
      </DetailsLayout>
    </ReactFlowProvider>
  );
};
