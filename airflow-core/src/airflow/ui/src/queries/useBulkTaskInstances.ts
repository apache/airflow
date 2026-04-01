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
  useDagRunServiceGetDagRunsKey,
  useTaskInstanceServiceBulkTaskInstances,
  useTaskInstanceServiceGetTaskInstancesKey,
} from "openapi/queries";
import type {
  BulkActionResponse,
  BulkBody_BulkTaskInstanceBody_,
  BulkResponse,
} from "openapi/requests/types.gen";
import { toaster } from "src/components/ui";

type Props = {
  readonly clearSelections: VoidFunction;
  readonly onSuccessConfirm: VoidFunction;
};

const handleActionResult = (
  actionResult: BulkActionResponse,
  setError: (error: unknown) => void,
  onSuccess: (count: number, keys: Array<string>) => void,
) => {
  const { errors, success } = actionResult;

  if (Array.isArray(errors) && errors.length > 0) {
    const apiError = errors[0] as { error: string };

    setError({ body: { detail: apiError.error } });
  } else if (Array.isArray(success) && success.length > 0) {
    onSuccess(success.length, success);
  }
};

export const useBulkTaskInstances = ({ clearSelections, onSuccessConfirm }: Props) => {
  const queryClient = useQueryClient();
  const [error, setError] = useState<unknown>(undefined);
  const { t: translate } = useTranslation(["common", "dags"]);

  const onSuccess = async (responseData: BulkResponse) => {
    await Promise.all([
      queryClient.invalidateQueries({ queryKey: [useTaskInstanceServiceGetTaskInstancesKey] }),
      queryClient.invalidateQueries({ queryKey: [useDagRunServiceGetDagRunsKey] }),
    ]);

    const isDelete = Boolean(responseData.delete);
    const actionResult = responseData.delete ?? responseData.update;
    const toasterKey = isDelete ? "toaster.bulkDelete" : "toaster.bulkUpdate";

    if (actionResult) {
      handleActionResult(actionResult, setError, (count, keys) => {
        toaster.create({
          description: translate(`${toasterKey}.success.description`, {
            count,
            keys: keys.join(", "),
            resourceName: translate("taskInstance_other"),
          }),
          title: translate(`${toasterKey}.success.title`, {
            resourceName: translate("taskInstance_other"),
          }),
          type: "success",
        });
        clearSelections();
        onSuccessConfirm();
      });
    }
  };

  const onError = (_error: unknown) => {
    setError(_error);
  };

  const { isPending, mutate } = useTaskInstanceServiceBulkTaskInstances({
    onError,
    onSuccess,
  });

  const bulkAction = (requestBody: BulkBody_BulkTaskInstanceBody_) => {
    setError(undefined);
    mutate({ dagId: "~", dagRunId: "~", requestBody });
  };

  return { bulkAction, error, isPending, setError };
};
