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
  useDagRunServicePostClearDagRuns,
  useTaskInstanceServiceGetTaskInstancesKey,
} from "openapi/queries";
import type { DAGRunResponse } from "openapi/requests/types.gen";
import { toaster } from "src/components/ui";
import type { BulkErrorEntry } from "src/pages/DagRuns/bulkActionTypes";

type Props = {
  readonly clearSelections: VoidFunction;
  readonly onSuccessConfirm: VoidFunction;
};

export type BulkClearDagRunsOptions = {
  note: string | null;
  onlyFailed: boolean;
  onlyNew: boolean;
  runOnLatestVersion: boolean;
};

export const useBulkClearDagRuns = ({ clearSelections, onSuccessConfirm }: Props) => {
  const queryClient = useQueryClient();
  const { t: translate } = useTranslation(["common", "dags"]);
  const [actionErrors, setActionErrors] = useState<Array<BulkErrorEntry>>([]);

  const mutation = useDagRunServicePostClearDagRuns({
    onSuccess: async (response) => {
      const errors = (response.errors ?? []) as Array<BulkErrorEntry>;
      const successes = response.success ?? [];

      if (successes.length > 0) {
        await Promise.all([
          queryClient.invalidateQueries({ queryKey: [useDagRunServiceGetDagRunsKey] }),
          queryClient.invalidateQueries({ queryKey: [useTaskInstanceServiceGetTaskInstancesKey] }),
        ]);

        toaster.create({
          description: translate("toaster.bulkClear.success.description", {
            count: successes.length,
            keys: successes.join(", "),
            resourceName: translate("dagRun_other"),
          }),
          title: translate("toaster.bulkClear.success.title", {
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

  const clear = (dagRuns: Array<DAGRunResponse>, options: BulkClearDagRunsOptions) => {
    setActionErrors([]);
    mutation.mutate({
      dagId: "~",
      requestBody: {
        dry_run: false,
        note: options.note,
        only_failed: options.onlyFailed,
        only_new: options.onlyNew,
        run_on_latest_version: options.runOnLatestVersion,
        runs: dagRuns.map((dr) => ({ dag_id: dr.dag_id, dag_run_id: dr.dag_run_id })),
      },
    });
  };

  const reset = () => {
    setActionErrors([]);
    mutation.reset();
  };

  return {
    actionErrors,
    clear,
    error: mutation.error,
    isPending: mutation.isPending,
    reset,
  };
};
