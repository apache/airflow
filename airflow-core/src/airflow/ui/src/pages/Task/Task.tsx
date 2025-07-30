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
import { LuChartColumn } from "react-icons/lu";
import { MdOutlineEventNote, MdOutlineTask } from "react-icons/md";
import { useParams } from "react-router-dom";

import { useDagServiceGetDagDetails, useGridServiceGridData, useTaskServiceGetTask } from "openapi/queries";
import { DetailsLayout } from "src/layouts/Details/DetailsLayout";
import { getGroupTask } from "src/utils/groupTask";

import { GroupTaskHeader } from "./GroupTaskHeader";
import { Header } from "./Header";

export const Task = () => {
  const { t: translate } = useTranslation("dag");
  const { dagId = "", groupId, taskId } = useParams();

  const tabs = [
    { icon: <LuChartColumn />, label: translate("tabs.overview"), value: "" },
    { icon: <MdOutlineTask />, label: translate("tabs.taskInstances"), value: "task_instances" },
    { icon: <MdOutlineEventNote />, label: translate("tabs.auditLog"), value: "events" },
  ];

  const displayTabs = groupId === undefined ? tabs : tabs.filter((tab) => tab.value !== "events");

  const {
    data: task,
    error,
    isLoading,
  } = useTaskServiceGetTask({ dagId, taskId: groupId ?? taskId }, undefined, {
    enabled: groupId === undefined,
  });

  const { data: gridData } = useGridServiceGridData(
    {
      dagId,
      includeDownstream: true,
      includeUpstream: true,
    },
    undefined,
    { enabled: groupId !== undefined },
  );

  const groupTask =
    groupId === undefined ? undefined : getGroupTask(gridData?.structure.nodes ?? [], groupId);

  const {
    data: dag,
    error: dagError,
    isLoading: isDagLoading,
  } = useDagServiceGetDagDetails({
    dagId,
  });

  return (
    <ReactFlowProvider>
      <DetailsLayout
        dag={dag}
        error={error ?? dagError}
        isLoading={isLoading || isDagLoading}
        tabs={displayTabs}
      >
        {task === undefined ? undefined : <Header task={task} />}
        {groupTask ? <GroupTaskHeader groupTask={groupTask} /> : undefined}
      </DetailsLayout>
    </ReactFlowProvider>
  );
};
