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
import { useTaskInstanceServiceGetHitlDetails, useTaskServiceGetTasks } from "openapi/queries";

type HITLDetailsParams = {
  dagId?: string;
  dagRunId?: string;
  responseReceived?: boolean;
  state?: Array<string>;
  taskId?: string;
  taskIdPattern?: string;
};

type HITLDetailsOptions = {
  refetchInterval?: number | false;
};

export const useHitlDetails = (params: HITLDetailsParams, options: HITLDetailsOptions = {}) => {
  const { dagId, dagRunId, responseReceived, state, taskId, taskIdPattern } = params;
  const { refetchInterval } = options;

  // If there is a dagId, let's see if any tasks have HITL operators
  const { data: tasksData } = useTaskServiceGetTasks({ dagId: dagId ?? "~" }, undefined, {
    enabled: Boolean(dagId),
  });

  const hasHitlOperators = Boolean(
    tasksData?.tasks.some((task) => {
      const operatorName = task.operator_name;

      // TODO: do this in the backend
      return (
        operatorName === "HITLOperator" ||
        operatorName === "ApprovalOperator" ||
        operatorName === "HITLEntryOperator" ||
        operatorName === "HITLBranchOperator"
      );
    }),
  );

  return useTaskInstanceServiceGetHitlDetails(
    {
      dagId: dagId ?? "~",
      dagRunId: dagRunId ?? "~",
      responseReceived,
      state,
      taskId,
      taskIdPattern,
    },
    undefined,
    {
      // Only refetch if any tasks have HITL operators
      // keeping enabled in case the dag used to have HITL operators in a previous version
      refetchInterval: hasHitlOperators ? refetchInterval : false,
    },
  );
};
