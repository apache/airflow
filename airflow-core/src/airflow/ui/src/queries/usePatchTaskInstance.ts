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
import { useMutation } from "@tanstack/react-query";
import { useQueryClient } from "@tanstack/react-query";
import { useTranslation } from "react-i18next";

import {
  UseTaskInstanceServiceGetMappedTaskInstanceKeyFn,
  useTaskInstanceServiceGetTaskInstancesKey,
  UseGridServiceGetGridRunsKeyFn,
  UseGridServiceGetGridTiSummariesKeyFn,
  useGridServiceGetGridTiSummariesKey,
  useTaskInstanceServicePatchTaskInstances,
} from "openapi/queries";
import { TaskInstanceService } from "openapi/requests/services.gen";
import type { PatchTaskInstancesBody, TaskInstanceCollectionResponse } from "openapi/requests/types.gen";
import { toaster } from "src/components/ui";

import { useClearTaskInstancesDryRunKey } from "./useClearTaskInstancesDryRun";
import { usePatchTaskInstanceDryRunKey } from "./usePatchTaskInstanceDryRun";

export const usePatchTaskInstance = ({
  dagId,
  dagRunId,
  onSuccess,
}: {
  dagId: string;
  dagRunId: string;
  onSuccess?: () => void;
}) => {
  const queryClient = useQueryClient();
  const { t: translate } = useTranslation();

  const onError = (error: Error) => {
    toaster.create({
      description: error.message,
      title: translate("toaster.update.error", {
        resourceName: translate("taskInstance_one"),
      }),
      type: "error",
    });
  };

  const onSuccessFn = async (
    _: TaskInstanceCollectionResponse,
    variables: {
      dagId: string;
      dagRunId: string;
      requestBody: PatchTaskInstancesBody;
    },
  ) => {
    // Deduplication using set as user can patch multiple map index of the same task_id.
    const taskInstanceKeys = [
      ...new Set(
        variables.requestBody.task_ids
          .filter((taskId) => typeof taskId === "string" || Array.isArray(taskId))
          .map((taskId) => {
            const [actualTaskId, mapIndex] = Array.isArray(taskId) ? taskId : [taskId, undefined];
            const params = { dagId, dagRunId, mapIndex: mapIndex ?? -1, taskId: actualTaskId };
            return UseTaskInstanceServiceGetMappedTaskInstanceKeyFn(params);
          }),
      ),
    ];

    // Check if this patch operation affects multiple DAG runs
    const { include_future: includeFuture, include_past: includePast } = variables.requestBody;
    const affectsMultipleRuns = includeFuture === true || includePast === true;

    const queryKeys = [
      ...taskInstanceKeys,
      [useTaskInstanceServiceGetTaskInstancesKey],
      [usePatchTaskInstanceDryRunKey, dagId, dagRunId],
      [useClearTaskInstancesDryRunKey, dagId],
      UseGridServiceGetGridRunsKeyFn({ dagId }, [{ dagId }]),
      affectsMultipleRuns
        ? [useGridServiceGetGridTiSummariesKey, { dagId }]
        : UseGridServiceGetGridTiSummariesKeyFn({ dagId, runId: dagRunId }),
    ];

    await Promise.all(queryKeys.map((key) => queryClient.invalidateQueries({ queryKey: key })));

    if (onSuccess) {
      onSuccess();
    }
  };

  return useTaskInstanceServicePatchTaskInstances({
    onError,
    onSuccess: onSuccessFn,
  });
};
