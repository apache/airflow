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
  useDagRunServiceBulkDagRuns,
  useDagRunServiceGetDagRunsKey,
  useTaskInstanceServiceGetTaskInstancesKey,
} from "openapi/queries";
import type { BulkResponse } from "openapi/requests/types.gen";
import { toaster } from "src/components/ui";

type Props = {
  readonly clearSelections: VoidFunction;
  readonly onSuccessConfirm: VoidFunction;
};

export type BulkActionError = { error: string; status_code?: number };

export const useBulkDeleteDagRuns = ({ clearSelections, onSuccessConfirm }: Props) => {
  const queryClient = useQueryClient();
  const { t: translate } = useTranslation(["common", "dags"]);

  const onSuccess = async (responseData: BulkResponse) => {
    const successKeys = responseData.delete?.success ?? [];
    const errors = (responseData.delete?.errors ?? []) as Array<BulkActionError>;

    // Only invalidate when something actually got deleted — a 200 with all-errors
    // shouldn't churn the table.
    if (successKeys.length > 0) {
      await Promise.all([
        queryClient.invalidateQueries({ queryKey: [useDagRunServiceGetDagRunsKey] }),
        queryClient.invalidateQueries({ queryKey: [useTaskInstanceServiceGetTaskInstancesKey] }),
      ]);

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
      clearSelections();
    }

    // Keep the dialog open if any per-entity error came back so the user can see what failed.
    if (errors.length === 0) {
      onSuccessConfirm();
    }
  };

  const { data, error, isPending, mutate, reset } = useDagRunServiceBulkDagRuns({ onSuccess });

  return {
    actionErrors: (data?.delete?.errors ?? []) as Array<BulkActionError>,
    bulkDelete: (dagRuns: Array<{ dag_id: string; dag_run_id: string }>) =>
      mutate({
        dagId: "~",
        requestBody: {
          actions: [
            {
              action: "delete" as const,
              action_on_non_existence: "skip",
              entities: dagRuns.map((dagRun) => ({
                dag_id: dagRun.dag_id,
                dag_run_id: dagRun.dag_run_id,
              })),
            },
          ],
        },
      }),
    error,
    isPending,
    reset,
  };
};
