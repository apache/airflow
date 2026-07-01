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
import { useRef } from "react";
import { useTranslation } from "react-i18next";

import {
  useDagRunServiceBulkDagRuns,
  useDagRunServiceGetDagRunsKey,
  useTaskInstanceServiceGetTaskInstancesKey,
} from "openapi/queries";
import type { BulkBody_BulkDAGRunBody_, BulkResponse } from "openapi/requests/types.gen";
import { toaster } from "src/components/ui";

import { gridQueryKeys, tiPerAttemptQueryKeys } from "./gridViewQueryKeys";

type Props = {
  readonly deselectKeys: (keys: Array<string>) => void;
  readonly onSuccessConfirm: VoidFunction;
};

export const useBulkDeleteDagRuns = ({ deselectKeys, onSuccessConfirm }: Props) => {
  const queryClient = useQueryClient();
  const affectedDagIds = useRef<Set<string>>(new Set());
  const { t: translate } = useTranslation(["common", "dags"]);

  const onSuccess = async (responseData: BulkResponse) => {
    await Promise.all([
      queryClient.invalidateQueries({ queryKey: [useDagRunServiceGetDagRunsKey] }),
      queryClient.invalidateQueries({ queryKey: [useTaskInstanceServiceGetTaskInstancesKey] }),
      ...tiPerAttemptQueryKeys.map((key) => queryClient.invalidateQueries({ queryKey: key })),
      ...[...affectedDagIds.current].flatMap((dagId) =>
        gridQueryKeys(dagId).map((key) => queryClient.invalidateQueries({ queryKey: key })),
      ),
    ]);

    const deleteResult = responseData.delete;

    if (!deleteResult) {
      return;
    }

    const successKeys = deleteResult.success ?? [];
    const actionErrors = deleteResult.errors ?? [];

    if (successKeys.length > 0) {
      toaster.create({
        description: translate("toaster.bulkDelete.success.description", {
          count: successKeys.length,
          keys: successKeys.join(", "),
          resourceName: translate("dagRun_other"),
        }),
        title: translate("toaster.bulkDelete.success.title", {
          resourceName: translate("dagRun_other"),
        }),
        type: "success",
      });
      deselectKeys(successKeys);
    }

    // Per-entity failures (status 200 with items in ``errors``) keep the dialog open
    // so the user can see what failed; the consumer renders ``data.delete.errors``.
    if (actionErrors.length === 0) {
      onSuccessConfirm();
    }
  };

  const { data, error, isPending, mutate, reset } = useDagRunServiceBulkDagRuns({ onSuccess });

  const bulkAction = (requestBody: BulkBody_BulkDAGRunBody_) => {
    reset();
    const dagIds = new Set<string>();

    for (const action of requestBody.actions) {
      for (const entity of action.entities) {
        if (typeof entity !== "string" && entity.dag_id !== null && entity.dag_id !== undefined) {
          dagIds.add(entity.dag_id);
        }
      }
    }
    affectedDagIds.current = dagIds;
    mutate({ dagId: "~", requestBody });
  };

  return { bulkAction, data, error, isPending, reset };
};
