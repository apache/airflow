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
import { MdOutlineTask } from "react-icons/md";
import { useParams } from "react-router-dom";

import {
  useDagRunServiceGetDagRun,
  useDagServiceGetDagDetails,
  useGridServiceGridData,
} from "openapi/queries";
import { DetailsLayout } from "src/layouts/Details/DetailsLayout";
import { isStatePending, useAutoRefresh } from "src/utils";

import { Header } from "./Header";

export const MappedTaskInstance = () => {
  const { dagId = "", runId = "", taskId = "" } = useParams();
  const refetchInterval = useAutoRefresh({ dagId });

  const {
    data: dag,
    error: dagError,
    isLoading: isDagLoading,
  } = useDagServiceGetDagDetails({
    dagId,
  });

  const { data: dagRun } = useDagRunServiceGetDagRun(
    {
      dagId,
      dagRunId: runId,
    },
    undefined,
    { enabled: runId !== "" },
  );

  // Filter grid data to get only a single dag run
  const { data, error, isLoading } = useGridServiceGridData(
    {
      dagId,
      limit: 1,
      offset: 0,
      runAfterGte: dagRun?.run_after,
      runAfterLte: dagRun?.run_after,
    },
    undefined,
    {
      enabled: dagRun !== undefined,
      refetchInterval: (query) =>
        query.state.data?.dag_runs.some((dr) => isStatePending(dr.state)) && refetchInterval,
    },
  );

  const taskInstance = data?.dag_runs
    .find((dr) => dr.dag_run_id === runId)
    ?.task_instances.find((ti) => ti.task_id === taskId);

  const tabs = [
    { icon: <MdOutlineTask />, label: `Task Instances [${taskInstance?.task_count}]`, value: "" },
  ];

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
