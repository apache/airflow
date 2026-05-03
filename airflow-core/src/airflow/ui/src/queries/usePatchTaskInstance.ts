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
  UseTaskInstanceServiceGetMappedTaskInstanceKeyFn,
  useTaskInstanceServiceGetMappedTaskInstanceKey,
  UseTaskInstanceServiceGetTaskInstanceKeyFn,
  useTaskInstanceServiceGetTaskInstancesKey,
  useTaskInstanceServicePatchTaskInstance,
} from "openapi/queries";
import { createErrorToaster } from "src/utils";

import { gridQueryKeys } from "./gridViewQueryKeys";
import { useClearTaskInstancesDryRunKey } from "./useClearTaskInstancesDryRun";
import { usePatchTaskInstanceDryRunKey } from "./usePatchTaskInstanceDryRun";

export const usePatchTaskInstance = ({
  dagId,
  dagRunId,
  mapIndex,
  onSuccess,
  taskId,
}: {
  dagId: string;
  dagRunId: string;
  mapIndex?: number;
  onSuccess?: () => void;
  taskId: string;
}) => {
  const queryClient = useQueryClient();
  const { t: translate } = useTranslation();

  const onError = (error: unknown) => {
    createErrorToaster(
      error,
      {
        params: { resourceName: translate("taskInstance_one") },
        titleKey: "toaster.update.error",
      },
      translate,
    );
  };

  const onSuccessFn = async () => {
    const queryKeys = [
      UseTaskInstanceServiceGetTaskInstanceKeyFn({ dagId, dagRunId, taskId }),
      [useTaskInstanceServiceGetTaskInstancesKey],
      [usePatchTaskInstanceDryRunKey, dagId, dagRunId, { mapIndex, taskId }],
      [useClearTaskInstancesDryRunKey, dagId],
    ];

    if (mapIndex !== undefined) {
      queryKeys.push(UseTaskInstanceServiceGetMappedTaskInstanceKeyFn({ dagId, dagRunId, mapIndex, taskId }));
    }

    await Promise.all([
      ...gridQueryKeys(dagId).map((key) => queryClient.invalidateQueries({ queryKey: key })),
      ...queryKeys.map((key) => queryClient.invalidateQueries({ queryKey: key })),
      // Wildcard match when patching every mapped TI (mapIndex undefined):
      // invalidate the mapped-TI cache for every map_index of this task.
      // The mapped-TI key embeds its params as a single object, so prefix
      // matching on a partial key doesn't catch them — match via predicate.
      ...(mapIndex === undefined
        ? [
            queryClient.invalidateQueries({
              predicate: ({ queryKey }) => {
                if (queryKey[0] !== useTaskInstanceServiceGetMappedTaskInstanceKey) {
                  return false;
                }
                const params = queryKey[1] as
                  | { dagId?: string; dagRunId?: string; taskId?: string }
                  | undefined;

                return params?.dagId === dagId && params.dagRunId === dagRunId && params.taskId === taskId;
              },
            }),
          ]
        : []),
    ]);

    if (onSuccess) {
      onSuccess();
    }
  };

  return useTaskInstanceServicePatchTaskInstance({
    onError,
    onSuccess: onSuccessFn,
  });
};
