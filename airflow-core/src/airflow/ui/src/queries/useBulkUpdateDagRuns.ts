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
import type { DAGRunPatchStates, DAGRunResponse } from "openapi/requests/types.gen";
import { toaster } from "src/components/ui";
import type { BulkErrorEntry } from "src/pages/DagRuns/bulkActionTypes";

type Props = {
  readonly clearSelections: VoidFunction;
  readonly onSuccessConfirm: VoidFunction;
};

export type BulkUpdateDagRunsOptions = {
  note: string | null;
  state: DAGRunPatchStates;
};

export const useBulkUpdateDagRuns = ({ clearSelections, onSuccessConfirm }: Props) => {
  const queryClient = useQueryClient();
  const { t: translate } = useTranslation(["common", "dags"]);
  const [actionErrors, setActionErrors] = useState<Array<BulkErrorEntry>>([]);

  const mutation = useDagRunServiceBulkDagRuns({
    onSuccess: async (response) => {
      const errors = (response.update?.errors ?? []) as Array<BulkErrorEntry>;
      const successes = response.update?.success ?? [];

      if (successes.length > 0) {
        await Promise.all([
          queryClient.invalidateQueries({ queryKey: [useDagRunServiceGetDagRunsKey] }),
          queryClient.invalidateQueries({ queryKey: [useTaskInstanceServiceGetTaskInstancesKey] }),
        ]);

        toaster.create({
          description: translate("toaster.bulkUpdate.success.description", {
            count: successes.length,
            keys: successes.join(", "),
            resourceName: translate("dagRun_other"),
          }),
          title: translate("toaster.bulkUpdate.success.title", {
            resourceName: translate("dagRun_other"),
          }),
          type: "success",
        });
      }

      if (errors.length > 0) {
        setActionErrors(errors);
      } else {
        clearSelections();
        onSuccessConfirm();
      }
    },
  });

  const update = (dagRuns: Array<DAGRunResponse>, options: BulkUpdateDagRunsOptions) => {
    setActionErrors([]);
    const updateMask = options.note === null ? ["state"] : ["state", "note"];

    mutation.mutate({
      dagId: "~",
      requestBody: {
        actions: [
          {
            action: "update" as const,
            action_on_non_existence: "skip",
            entities: dagRuns.map((dr) => ({
              dag_id: dr.dag_id,
              dag_run_id: dr.dag_run_id,
              note: options.note,
              state: options.state,
            })),
            update_mask: updateMask,
          },
        ],
      },
    });
  };

  const reset = () => {
    setActionErrors([]);
    mutation.reset();
  };

  return {
    actionErrors,
    error: mutation.error,
    isPending: mutation.isPending,
    reset,
    update,
  };
};
