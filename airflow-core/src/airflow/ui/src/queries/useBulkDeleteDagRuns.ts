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
  useDagRunServiceBulkDagRuns,
  useDagRunServiceGetDagRunsKey,
  useTaskInstanceServiceGetTaskInstancesKey,
} from "openapi/queries";
import type { BulkActionResponse, BulkBody_BulkDAGRunBody_, BulkResponse } from "openapi/requests/types.gen";
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

export const useBulkDeleteDagRuns = ({ clearSelections, onSuccessConfirm }: Props) => {
  const queryClient = useQueryClient();
  const [error, setError] = useState<unknown>(undefined);
  const { t: translate } = useTranslation(["common", "dags"]);

  const onSuccess = async (responseData: BulkResponse) => {
    await Promise.all([
      queryClient.invalidateQueries({ queryKey: [useDagRunServiceGetDagRunsKey] }),
      queryClient.invalidateQueries({ queryKey: [useTaskInstanceServiceGetTaskInstancesKey] }),
    ]);

    if (responseData.delete) {
      handleActionResult(responseData.delete, setError, (count, keys) => {
        toaster.create({
          description: translate("toaster.bulkDelete.success.description", {
            count,
            keys: keys.join(", "),
            resourceName: translate("dagRun_other"),
          }),
          title: translate("toaster.bulkDelete.success.title", {
            resourceName: translate("dagRun_other"),
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

  const { isPending, mutate } = useDagRunServiceBulkDagRuns({
    onError,
    onSuccess,
  });

  const bulkAction = (requestBody: BulkBody_BulkDAGRunBody_) => {
    setError(undefined);
    mutate({ dagId: "~", requestBody });
  };

  return { bulkAction, error, isPending, setError };
};
