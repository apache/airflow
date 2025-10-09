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
  UseDagRunServiceGetDagRunKeyFn,
  useDagRunServiceGetDagRunsKey,
  UseTaskInstanceServiceGetMappedTaskInstanceKeyFn,
  useTaskInstanceServicePostClearTaskInstances,
  UseGridServiceGetGridRunsKeyFn,
  UseGridServiceGetGridTiSummariesKeyFn,
  useGridServiceGetGridTiSummariesKey,
} from "openapi/queries";
import type { ClearTaskInstancesBody, TaskInstanceCollectionResponse } from "openapi/requests/types.gen";
import { toaster } from "src/components/ui";

import { useClearTaskInstancesDryRunKey } from "./useClearTaskInstancesDryRun";
import { usePatchTaskInstanceDryRunKey } from "./usePatchTaskInstanceDryRun";

export const useClearTaskInstances = ({
  dagId,
  dagRunId,
  onSuccessConfirm,
}: {
  dagId: string;
  dagRunId: string;
  onSuccessConfirm: () => void;
}) => {
  const queryClient = useQueryClient();
  const { t: translate } = useTranslation("dags");

  const onError = (error: Error) => {
    toaster.create({
      description: error.message,
      title: translate("dags:runAndTaskActions.clear.error", { type: translate("taskInstance_one") }),
      type: "error",
    });
  };

  const onSuccess = async (
    _: TaskInstanceCollectionResponse,
    variables: { dagId: string; requestBody: ClearTaskInstancesBody },
  ) => {
    // deduplication using set as user can clear multiple map index of the same task_id.
    const taskInstanceKeys = [
      ...new Set(
        (variables.requestBody.task_ids ?? [])
          .filter((taskId) => typeof taskId === "string" || Array.isArray(taskId))
          .map((taskId) => {
            const [actualTaskId, mapIndex] = Array.isArray(taskId) ? taskId : [taskId, undefined];
            const runId = variables.requestBody.dag_run_id;

            if (runId === null || runId === undefined) {
              return undefined;
            }

            const params = { dagId, dagRunId: runId, mapIndex: mapIndex ?? -1, taskId: actualTaskId };

            return UseTaskInstanceServiceGetMappedTaskInstanceKeyFn(params);
          })
          .filter((key) => key !== undefined),
      ),
    ];

    // Check if this clear operation affects multiple DAG runs
    const { include_future: includeFuture, include_past: includePast } = variables.requestBody;
    const affectsMultipleRuns = includeFuture === true || includePast === true;

    const queryKeys = [
      ...taskInstanceKeys,
      UseDagRunServiceGetDagRunKeyFn({ dagId, dagRunId }),
      [useDagRunServiceGetDagRunsKey],
      [useClearTaskInstancesDryRunKey, dagId],
      [usePatchTaskInstanceDryRunKey, dagId, dagRunId],
      UseGridServiceGetGridRunsKeyFn({ dagId }, [{ dagId }]),
      affectsMultipleRuns
        ? [useGridServiceGetGridTiSummariesKey, { dagId }]
        : UseGridServiceGetGridTiSummariesKeyFn({ dagId, runId: dagRunId }),
    ];

    await Promise.all(queryKeys.map((key) => queryClient.invalidateQueries({ queryKey: key })));

    onSuccessConfirm();
  };

  return useTaskInstanceServicePostClearTaskInstances({
    onError,
    onSuccess,
  });
};
