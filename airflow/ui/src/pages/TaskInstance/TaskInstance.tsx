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
import { FiCode } from "react-icons/fi";
import { MdDetails, MdOutlineEventNote, MdReorder, MdSyncAlt } from "react-icons/md";
import { PiBracketsCurlyBold } from "react-icons/pi";
import { useParams } from "react-router-dom";

import { useDagServiceGetDagDetails, useTaskInstanceServiceGetMappedTaskInstance } from "openapi/queries";
import { DetailsLayout } from "src/layouts/Details/DetailsLayout";
import { isStatePending, useAutoRefresh } from "src/utils";

import { Header } from "./Header";

const tabs = [
  { icon: <MdReorder />, label: "Logs", value: "" },
  { icon: <PiBracketsCurlyBold />, label: "Rendered Templates", value: "rendered_templates" },
  { icon: <MdSyncAlt />, label: "XCom", value: "xcom" },
  { icon: <MdOutlineEventNote />, label: "Events", value: "events" },
  { icon: <FiCode />, label: "Code", value: "code" },
  { icon: <MdDetails />, label: "Details", value: "details" },
];

export const TaskInstance = () => {
  const { dagId = "", mapIndex = "-1", runId = "", taskId = "" } = useParams();

  const refetchInterval = useAutoRefresh({ dagId });

  const {
    data: dag,
    error: dagError,
    isLoading: isDagLoading,
  } = useDagServiceGetDagDetails({
    dagId,
  });

  const {
    data: taskInstance,
    error,
    isLoading,
  } = useTaskInstanceServiceGetMappedTaskInstance(
    {
      dagId,
      dagRunId: runId,
      mapIndex: parseInt(mapIndex, 10),
      taskId,
    },
    undefined,
    {
      refetchInterval: (query) => (isStatePending(query.state.data?.state) ? refetchInterval : false),
    },
  );

  return (
    <ReactFlowProvider>
      <DetailsLayout dag={dag} error={error ?? dagError} isLoading={isLoading || isDagLoading} tabs={tabs}>
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
