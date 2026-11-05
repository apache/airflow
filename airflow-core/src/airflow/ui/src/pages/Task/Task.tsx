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

import { DetailsLayout } from "src/layouts/Details/DetailsLayout";

import { HeaderCardSkeleton } from "src/components/HeaderCardSkeleton";

import { usePluginTabs } from "src/hooks/usePluginTabs";
import { useRequiredActionTabs } from "src/hooks/useRequiredActionTabs";
import { useGridStructure } from "src/queries/useGridStructure.ts";
import { useDocumentTitle, useStickyError } from "src/utils";
import { getGroupTask } from "src/utils/groupTask";

import { GroupTaskHeader } from "./GroupTaskHeader";
import { Header, HeaderError, TASK_STAT_SKELETON } from "./Header";

export const Task = () => {
  const { t: translate } = useTranslation(["dag", "hitl"]);
  const { dagId = "", groupId, runId, taskId } = useParams();

  useDocumentTitle(groupId ?? taskId);

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

  const shownError = useStickyError(error, task !== undefined, `${dagId}/${taskId ?? ""}`);

  return (
    <ReactFlowProvider>
      <DetailsLayout tabs={displayTabs}>
        {/* A group has no task to fetch, so neither the skeleton nor the error applies to one.
          The failure is tested first and read through useStickyError, because a refetch of a task
          that never loaded clears the error and reports itself as pending again. */}
        {groupId === undefined && task === undefined && Boolean(shownError) ? (
          <HeaderError error={shownError} taskId={taskId ?? ""} />
        ) : undefined}
        {groupId === undefined && task === undefined && !Boolean(shownError) && isLoading ? (
          <HeaderCardSkeleton stats={TASK_STAT_SKELETON} type="task" />
        ) : undefined}
        {task === undefined ? undefined : <Header error={shownError} task={task} />}
        {groupTask ? <GroupTaskHeader docMd={groupTask.doc_md} title={groupTask.label} /> : undefined}
      </DetailsLayout>
    </ReactFlowProvider>
  );
};
