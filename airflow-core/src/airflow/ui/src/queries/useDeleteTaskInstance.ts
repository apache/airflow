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
  useTaskInstanceServiceDeleteTaskInstance,
  useTaskInstanceServiceGetTaskInstanceKey,
  useTaskInstanceServiceGetTaskInstancesKey,
  useDagRunServiceGetDagRunsKey,
  UseDagRunServiceGetDagRunKeyFn,
  UseGridServiceGetGridTiSummariesKeyFn,
  useTaskInstanceServiceGetHitlDetailsKey,
} from "openapi/queries";
import { toaster } from "src/components/ui";

type DeleteTaskInstanceParams = {
  dagId: string;
  dagRunId: string;
  mapIndex?: number;
  onSuccessConfirm: () => void;
  taskId: string;
};

export const useDeleteTaskInstance = ({
  dagId,
  dagRunId,
  mapIndex,
  onSuccessConfirm,
  taskId,
}: DeleteTaskInstanceParams) => {
  const queryClient = useQueryClient();
  const { t: translate } = useTranslation(["common", "dags"]);

  const onError = (error: Error) => {
    // Get status from error
    const status =
      (error as unknown as { status?: number }).status ??
      (error as unknown as { response?: { status?: number } }).response?.status;

    // Skip 403 errors as they are handled by MutationCache
    if (status === 403) {
      return;
    }

    toaster.create({
      description: error.message,
      title: translate("dags:runAndTaskActions.delete.error", { type: translate("taskInstance_one") }),
      type: "error",
    });
  };

  const onSuccess = async () => {
    const queryKeys = [
      UseDagRunServiceGetDagRunKeyFn({ dagId, dagRunId }),
      UseGridServiceGetGridTiSummariesKeyFn({ dagId, runId: dagRunId }, [{ dagId, runId: dagRunId }]),
      [useDagRunServiceGetDagRunsKey],
      [useTaskInstanceServiceGetTaskInstancesKey],
      [useTaskInstanceServiceGetTaskInstanceKey, { dagId, dagRunId, mapIndex, taskId }],
      [useTaskInstanceServiceGetHitlDetailsKey],
    ];

    await Promise.all(queryKeys.map((key) => queryClient.invalidateQueries({ queryKey: key })));

    toaster.create({
      description: translate("dags:runAndTaskActions.delete.success.description", {
        type: translate("taskInstance_one"),
      }),
      title: translate("dags:runAndTaskActions.delete.success.title", {
        type: translate("taskInstance_one"),
      }),
      type: "success",
    });

    onSuccessConfirm();
  };

  return useTaskInstanceServiceDeleteTaskInstance({
    onError,
    onSuccess,
  });
};
