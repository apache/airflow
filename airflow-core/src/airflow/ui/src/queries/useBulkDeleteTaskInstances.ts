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
  useTaskInstanceServiceBulkTaskInstances,
  useTaskInstanceServiceGetTaskInstancesKey,
} from "openapi/queries";
import type { BulkResponse } from "openapi/requests/types.gen";
import { toaster } from "src/components/ui";

type Props = {
  readonly clearSelections: VoidFunction;
  readonly onSuccessConfirm: VoidFunction;
};

const parseTaskInstanceKey = (key: string) => {
  const [dagId = "", dagRunId = "", taskId = "", mapIndexStr] = key.split("||");

  return {
    dag_id: dagId,
    dag_run_id: dagRunId,
    map_index: mapIndexStr === undefined ? -1 : Number(mapIndexStr),
    task_id: taskId,
  };
};

export const useBulkDeleteTaskInstances = ({ clearSelections, onSuccessConfirm }: Props) => {
  const queryClient = useQueryClient();
  const [error, setError] = useState<unknown>(undefined);
  const { t: translate } = useTranslation(["common", "dags"]);

  const onSuccess = async (responseData: BulkResponse) => {
    await queryClient.invalidateQueries({
      queryKey: [useTaskInstanceServiceGetTaskInstancesKey],
    });

    if (responseData.delete) {
      const { errors, success } = responseData.delete;

      if (Array.isArray(errors) && errors.length > 0) {
        const apiError = errors[0] as { error: string };

        setError({ body: { detail: apiError.error } });
      } else if (Array.isArray(success) && success.length > 0) {
        toaster.create({
          description: translate("toaster.bulkDelete.success.description", {
            count: success.length,
            keys: success.join(", "),
            resourceName: translate("common:taskInstance_other"),
          }),
          title: translate("toaster.bulkDelete.success.title"),
          type: "success",
        });
        clearSelections();
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

  const bulkDelete = (selectedRows: Map<string, boolean>) => {
    mutate({
      dagId: "~",
      dagRunId: "~",
      requestBody: {
        actions: [
          {
            action: "delete" as const,
            action_on_non_existence: "skip",
            entities: [...selectedRows.keys()].map((key) => parseTaskInstanceKey(key)),
          },
        ],
      },
    });
  };

  return { bulkDelete, error, isPending };
};
