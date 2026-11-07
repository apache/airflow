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
import { useQueryClient } from "@tanstack/react-query";
import { useTranslation } from "react-i18next";

import {
  useDagRunServiceGetExecutionKey,
  useTaskInstanceServiceGetMappedTaskInstanceKey,
  UseDagRunServiceGetDagRunKeyFn,
  useDagRunServiceGetDagRunsKey,
  UseGanttServiceGetGanttDataKeyFn,
  useTaskInstanceServiceGetTaskInstancesKey,
  useTaskInstanceServicePostClearTaskInstances,
} from "openapi/queries";
import type { ClearTaskInstancesBody, TaskInstanceCollectionResponse } from "openapi/requests/types.gen";

import { createErrorToaster } from "src/utils";

import { dagListQueryKeys, gridQueryKeys, tiPerAttemptQueryKeys } from "./gridViewQueryKeys";
import { useClearTaskInstancesDryRunKey } from "./useClearTaskInstancesDryRun";
import { usePatchTaskInstanceDryRunKey } from "./usePatchTaskInstanceDryRun";

export const useClearTaskInstances = ({
  dagRunId,
  onSuccessConfirm,
}: {
  dagRunId: string;
  onSuccessConfirm: () => void;
}) => {
  const queryClient = useQueryClient();
  const { t: translate } = useTranslation("dags");

  const onError = (error: unknown) => {
    createErrorToaster(
      error,
      {
        params: { type: translate("common:taskInstance_one") },
        titleKey: "dags:runAndTaskActions.clear.error",
      },
      translate,
    );
  };

  const onSuccess = async (
    _: TaskInstanceCollectionResponse,
    variables: { dagId: string; requestBody: ClearTaskInstancesBody },
  ) => {
    const { dagId } = variables;
    const runId = variables.requestBody.dag_run_id ?? dagRunId;

    const queryKeys = [
      [useDagRunServiceGetExecutionKey, { dagId, dagRunId: runId }],
      [useTaskInstanceServiceGetMappedTaskInstanceKey, { dagId, dagRunId: runId }],
      UseDagRunServiceGetDagRunKeyFn({ dagId, dagRunId: runId }),
      [useDagRunServiceGetDagRunsKey],
      [useTaskInstanceServiceGetTaskInstancesKey],
      [useClearTaskInstancesDryRunKey, dagId],
      [usePatchTaskInstanceDryRunKey, dagId, runId],
      UseGanttServiceGetGanttDataKeyFn({ dagId, runId }),
      ...tiPerAttemptQueryKeys,
      ...dagListQueryKeys,
    ];

    await Promise.all([
      ...gridQueryKeys(dagId).map((key) => queryClient.invalidateQueries({ queryKey: key })),
      ...queryKeys.map((key) => queryClient.invalidateQueries({ queryKey: key })),
    ]);

    onSuccessConfirm();
  };

  return useTaskInstanceServicePostClearTaskInstances({
    onError,
    onSuccess,
    // This function uses the mutation function of React
    // For showing the error toast immediately, set retry to 0
    retry: 0,
  });
};
