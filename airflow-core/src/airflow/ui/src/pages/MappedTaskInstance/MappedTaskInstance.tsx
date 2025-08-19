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
import { MdOutlineTask } from "react-icons/md";
import { useParams } from "react-router-dom";

import { DetailsLayout } from "src/layouts/Details/DetailsLayout";
import { useGridTiSummaries } from "src/queries/useGridTISummaries.ts";
import { isStatePending, useAutoRefresh } from "src/utils";

import { Header } from "./Header";

export const MappedTaskInstance = () => {
  const { dagId = "", runId = "", taskId = "" } = useParams();
  const refetchInterval = useAutoRefresh({ dagId });
  const { t: translate } = useTranslation("dag");
  const { data: gridTISummaries } = useGridTiSummaries({ dagId, runId });

  const taskInstance = gridTISummaries?.task_instances.find((ti) => ti.task_id === taskId);
  let taskCount: number = 0;

  Object.entries(taskInstance?.child_states ?? {}).forEach(([, count]) => {
    taskCount += count;
  });

  const tabs = [
    { icon: <MdOutlineTask />, label: `${translate("tabs.taskInstances")} [${taskCount}]`, value: "" },
  ];

  return (
    <ReactFlowProvider>
      <DetailsLayout tabs={tabs}>
        {taskInstance === undefined ? undefined : (
          <Header
            isRefreshing={Boolean(isStatePending(taskInstance.state) && Boolean(refetchInterval))}
            taskInstance={taskInstance}
          />
        )}
      </DetailsLayout>
    </ReactFlowProvider>
  );
};
