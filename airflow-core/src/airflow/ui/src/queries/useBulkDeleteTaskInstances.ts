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
import { useState } from "react";
import { useTranslation } from "react-i18next";

import {
  useTaskInstanceServiceGetTaskInstancesKey,
  useTaskInstanceServiceBulkTaskInstances,
  UseGridServiceGetGridTiSummariesKeyFn,
} from "openapi/queries";
import type { TaskInstanceResponse } from "openapi/requests";
import { toaster } from "src/components/ui";

type Props = {
  readonly dagId?: string;
  readonly dagRunId?: string;
  readonly onSuccessConfirm: VoidFunction;
};

export const useBulkDeleteTaskInstances = ({ dagId, dagRunId, onSuccessConfirm }: Props) => {
  const queryClient = useQueryClient();
  const [error, setError] = useState<unknown>(undefined);
  const { t: translate } = useTranslation("common");

  const onSuccess = async (responseData: { delete?: { errors: Array<unknown>; success: Array<string> } }) => {
    const queryKeys = [
      [useTaskInstanceServiceGetTaskInstancesKey],
      dagId === undefined || dagRunId === undefined
        ? []
        : [UseGridServiceGetGridTiSummariesKeyFn({ dagId, runId: dagRunId })],
    ];

    await Promise.all(queryKeys.map((key) => queryClient.invalidateQueries({ queryKey: key })));

    if (responseData.delete) {
      const { errors, success } = responseData.delete;

      if (Array.isArray(errors) && errors.length > 0) {
        const apiError = errors[0] as { error: string };

        setError({
          body: { detail: apiError.error },
        });
      } else if (Array.isArray(success) && success.length > 0) {
        toaster.create({
          description: translate("bulkAction.delete.taskInstance.success.description", {
            count: success.length,
            keys: success.join(", "),
          }),
          title: translate("bulkAction.delete.taskInstance.success.title"),
          type: "success",
        });
        onSuccessConfirm();
      }
    }
  };

  const onError = (_error: unknown) => {
    setError(_error);
  };

  const { isPending, mutate } = useTaskInstanceServiceBulkTaskInstances({
    onError,
    onSuccess,
  });

  const deleteTaskInstances = (entities: Array<TaskInstanceResponse>) => {
    setError(undefined);

    const isSingleDagRun =
      Boolean(dagId) && Boolean(dagRunId) && dagId !== undefined && dagRunId !== undefined;

    mutate({
      dagId: isSingleDagRun ? dagId : "~",
      dagRunId: isSingleDagRun ? dagRunId : "~",
      requestBody: {
        actions: [
          {
            action: "delete",
            entities: entities.map((ti) => ({
              dag_id: isSingleDagRun ? undefined : ti.dag_id,
              dag_run_id: isSingleDagRun ? undefined : ti.dag_run_id,
              map_index: ti.map_index,
              task_id: ti.task_id,
            })),
          },
        ],
      },
    });
  };

  return { deleteTaskInstances, error, isPending };
};
