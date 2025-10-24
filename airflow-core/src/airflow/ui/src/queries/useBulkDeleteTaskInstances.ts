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
import { useState, useRef } from "react";
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

const SEPARATOR = "SEPARATOR";

export const useBulkDeleteTaskInstances = ({ dagId, dagRunId, onSuccessConfirm }: Props) => {
  const queryClient = useQueryClient();
  const [error, setError] = useState<unknown>(undefined);
  const { t: translate } = useTranslation("common");
  const pendingOperationsRef = useRef(0);
  const allSuccessesRef = useRef<Array<string>>([]);
  const hasErrorsRef = useRef(false);

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

        hasErrorsRef.current = true;
        setError({
          body: { detail: apiError.error },
        });
      } else if (Array.isArray(success) && success.length > 0) {
        allSuccessesRef.current = [...allSuccessesRef.current, ...success];
      }
    }

    pendingOperationsRef.current -= 1;

    if (pendingOperationsRef.current === 0 && !hasErrorsRef.current) {
      // All operations completed successfully
      if (allSuccessesRef.current.length > 0) {
        toaster.create({
          description: translate("bulkAction.delete.taskInstance.success.description", {
            count: allSuccessesRef.current.length,
            keys: allSuccessesRef.current.join(", "),
          }),
          title: translate("bulkAction.delete.taskInstance.success.title"),
          type: "success",
        });
        onSuccessConfirm();
      }
      // Reset state for next operation
      allSuccessesRef.current = [];
      hasErrorsRef.current = false;
    }
  };

  const onError = (_error: unknown) => {
    setError(_error);
    hasErrorsRef.current = true;
    pendingOperationsRef.current -= 1;
  };

  const { isPending, mutate } = useTaskInstanceServiceBulkTaskInstances({
    onError,
    onSuccess,
  });

  const deleteTaskInstances = (entities: Array<TaskInstanceResponse>) => {
    // Reset state for new operation
    allSuccessesRef.current = [];
    hasErrorsRef.current = false;
    setError(undefined);

    if (Boolean(dagId) && Boolean(dagRunId) && dagId !== undefined && dagRunId !== undefined) {
      pendingOperationsRef.current = 1;
      mutate({
        dagId,
        dagRunId,
        requestBody: {
          actions: [
            {
              action: "delete",
              entities: entities.map((ti) => ({ map_index: ti.map_index, task_id: ti.task_id })),
            },
          ],
        },
      });
    } else {
      // cross dag run
      const groupedByDagRunTIs: Record<string, Array<TaskInstanceResponse>> = {};

      entities.forEach((ti) => {
        (groupedByDagRunTIs[`${ti.dag_id}${SEPARATOR}${ti.dag_run_id}`] ??= []).push(ti);
      });

      pendingOperationsRef.current = Object.keys(groupedByDagRunTIs).length;

      Object.entries(groupedByDagRunTIs).forEach(([key, groupTIs]) => {
        const [groupDagId, groupDagRunId] = key.split(SEPARATOR);

        if (groupDagId !== undefined && groupDagRunId !== undefined) {
          mutate({
            dagId: groupDagId,
            dagRunId: groupDagRunId,
            requestBody: {
              actions: [
                {
                  action: "delete",
                  entities: groupTIs.map((ti) => ({ map_index: ti.map_index, task_id: ti.task_id })),
                },
              ],
            },
          });
        }
      });
    }
  };

  return { deleteTaskInstances, error, isPending };
};
