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

const SEPARATOR = "SEPARATOR";

export const useBulkPatchTaskInstances = ({ dagId, dagRunId, onSuccessConfirm }: Props) => {
  const queryClient = useQueryClient();
  const [error, setError] = useState<unknown>(undefined);
  const { t: translate } = useTranslation("common");

  const onSuccess = async (responseData: { patch?: { errors: Array<unknown>; success: Array<string> } }) => {
    const queryKeys = [
      [useTaskInstanceServiceGetTaskInstancesKey],
      dagId === undefined || dagRunId === undefined
        ? []
        : [UseGridServiceGetGridTiSummariesKeyFn({ dagId, runId: dagRunId })],
    ];

    await Promise.all(queryKeys.map((key) => queryClient.invalidateQueries({ queryKey: key })));

    if (responseData.patch) {
      const { errors, success } = responseData.patch;

      if (Array.isArray(errors) && errors.length > 0) {
        const apiError = errors[0] as { error: string };

        setError({
          body: { detail: apiError.error },
        });
      } else if (Array.isArray(success) && success.length > 0) {
        toaster.create({
          description: translate("bulkAction.patch.taskInstance.success.description", {
            count: success.length,
            keys: success.join(", "),
          }),
          title: translate("bulkAction.patch.taskInstance.success.title"),
          type: "success",
        });
        onSuccessConfirm();
      }
    }
  };

  const onError = (_error: unknown) => {
    setError(_error);
  };

  const getNewState = (action: string) => {
    switch (action) {
      case "set_failed":
        return "failed" as const;
      case "set_success":
        return "success" as const;
      default:
        return undefined;
    }
  };

  const { isPending, mutate } = useTaskInstanceServiceBulkTaskInstances({
    onError,
    onSuccess,
  });

  const patchTaskInstances = (
    entities: Array<TaskInstanceResponse>,
    selectedAction: string,
    options: {
      include_downstream?: boolean;
      include_future?: boolean;
      include_past?: boolean;
      include_upstream?: boolean;
      note?: string | null;
    } = {},
  ) => {
    const newState = getNewState(selectedAction);

    if (Boolean(dagId) && Boolean(dagRunId) && dagId !== undefined && dagRunId !== undefined) {
      mutate({
        dagId,
        dagRunId,
        requestBody: {
          actions: [
            {
              action: "update",
              entities: entities.map((ti) => ({
                include_downstream: options.include_downstream,
                include_future: options.include_future,
                include_past: options.include_past,
                include_upstream: options.include_upstream,
                map_index: ti.map_index,
                new_state: newState,
                note: options.note,
                task_id: ti.task_id,
              })),
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

      Object.entries(groupedByDagRunTIs).forEach(([key, groupTIs]) => {
        const [groupDagId, groupDagRunId] = key.split(SEPARATOR);

        if (groupDagId !== undefined && groupDagRunId !== undefined) {
          mutate({
            dagId: groupDagId,
            dagRunId: groupDagRunId,
            requestBody: {
              actions: [
                {
                  action: "update",
                  entities: groupTIs.map((ti) => ({
                    include_downstream: options.include_downstream,
                    include_future: options.include_future,
                    include_past: options.include_past,
                    include_upstream: options.include_upstream,
                    map_index: ti.map_index,
                    new_state: newState,
                    note: options.note,
                    task_id: ti.task_id,
                  })),
                },
              ],
            },
          });
        }
      });
    }
  };

  return { error, isPending, patchTaskInstances };
};
