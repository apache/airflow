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
import { useLocation, useParams } from "react-router-dom";

import type { LightGridTaskInstanceSummary } from "openapi/requests";
import { DetailsLayout } from "src/layouts/Details/DetailsLayout";
import { isStatePending, useAutoRefresh } from "src/utils";

import { Header } from "./Header";

type LocationState = {
  taskInstance: LightGridTaskInstanceSummary;
};

export const GroupTaskInstance = () => {
  const { dagId = "" } = useParams();

  const location = useLocation();
  const state = location.state as LocationState;
  const taskInstance: LightGridTaskInstanceSummary = state.taskInstance;
  const refetchInterval = useAutoRefresh({ dagId });

  const tabs = [{ icon: <MdOutlineTask />, label: "Task Instances", value: "" }];

  return (
    <ReactFlowProvider>
      <DetailsLayout tabs={tabs}>
        <Header
          isRefreshing={Boolean(isStatePending(taskInstance.state) && Boolean(refetchInterval))}
          taskInstance={taskInstance}
        />
      </DetailsLayout>
    </ReactFlowProvider>
  );
};
