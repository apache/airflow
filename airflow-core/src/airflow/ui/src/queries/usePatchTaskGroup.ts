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
  UseTaskInstanceServiceGetTaskInstanceKeyFn,
  useTaskInstanceServiceGetTaskInstancesKey,
  UseGridServiceGetGridRunsKeyFn,
  UseGridServiceGetGridTiSummariesKeyFn,
  useGridServiceGetGridTiSummariesKey,
} from "openapi/queries";
import { OpenAPI } from "openapi/requests/core/OpenAPI";
import { request as __request } from "openapi/requests/core/request";
import type { TaskInstanceCollectionResponse } from "openapi/requests/types.gen";
import { toaster } from "src/components/ui";

import { useClearTaskInstancesDryRunKey } from "./useClearTaskInstancesDryRun";
import { usePatchTaskGroupDryRunKey } from "./usePatchTaskGroupDryRun";

export const usePatchTaskGroup = ({
  dagId,
  dagRunId,
  onSuccess,
  taskGroupId,
}: {
  dagId: string;
  dagRunId: string;
  onSuccess?: () => void;
  taskGroupId: string;
}) => {
  const queryClient = useQueryClient();
  const { t: translate } = useTranslation();

  const onError = (error: Error) => {
    toaster.create({
      description: error.message,
      title: translate("toaster.update.error", {
        resourceName: translate("taskGroup"),
      }),
      type: "error",
    });
  };

  const onSuccessFn = async (
    _: unknown,
    variables: {
      dagId: string;
      dagRunId: string;
      requestBody: { include_future?: boolean; include_past?: boolean };
      taskGroupId: string;
    },
  ) => {
    // Check if this patch operation affects multiple DAG runs
    const { include_future: includeFuture, include_past: includePast } = variables.requestBody;
    const affectsMultipleRuns = includeFuture === true || includePast === true;

    const queryKeys = [
      UseTaskInstanceServiceGetTaskInstanceKeyFn({ dagId, dagRunId, taskId: taskGroupId }),
      [useTaskInstanceServiceGetTaskInstancesKey],
      [usePatchTaskGroupDryRunKey, dagId, dagRunId, taskGroupId],
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

  // Use direct API call until OpenAPI types are generated
  // TODO: Replace with useTaskInstanceServicePatchTaskGroup once OpenAPI types are generated
  return useMutation({
    mutationFn: async (variables: {
      dagId: string;
      dagRunId: string;
      requestBody: {
        include_downstream?: boolean;
        include_future?: boolean;
        include_past?: boolean;
        include_upstream?: boolean;
        new_state?: string;
        note?: string | null;
      };
      taskGroupId: string;
    }) =>
      __request(OpenAPI, {
        body: variables.requestBody,
        errors: {
          400: "Bad Request",
          401: "Unauthorized",
          403: "Forbidden",
          404: "Not Found",
          409: "Conflict",
          422: "Validation Error",
        },
        mediaType: "application/json",
        method: "PATCH",
        path: {
          dag_id: variables.dagId,
          dag_run_id: variables.dagRunId,
          identifier: variables.taskGroupId,
        },
        query: {
          task_group_id: variables.taskGroupId,
        },
        url: "/api/v2/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{identifier}",
      }) as Promise<TaskInstanceCollectionResponse>,
    onError,
    onSuccess: onSuccessFn,
  });
};
