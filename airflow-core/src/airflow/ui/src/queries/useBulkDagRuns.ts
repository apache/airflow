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

import { useDagRunServiceGetDagRunsKey, useTaskInstanceServiceGetTaskInstancesKey } from "openapi/queries";
import { DagRunService } from "openapi/requests/services.gen";
import type {
  BulkActionResponse,
  BulkResponse,
  DAGRunPatchStates,
  DAGRunResponse,
} from "openapi/requests/types.gen";
import { toaster } from "src/components/ui";

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

type BulkMarkOptions = {
  note: string | null;
  state: DAGRunPatchStates;
};

type ToasterKey = "toaster.bulkClear" | "toaster.bulkDelete" | "toaster.bulkUpdate";

const formatActionResult = (response: BulkActionResponse | null | undefined) => ({
  firstErrorDetail:
    response?.errors && response.errors.length > 0
      ? ((response.errors[0] as { error?: string } | undefined)?.error ?? "Bulk request failed")
      : null,
  successCount: response?.success?.length ?? 0,
  successKeys: response?.success ?? [],
});

export const useBulkDagRuns = ({ clearSelections, onSuccessConfirm }: Props) => {
  const queryClient = useQueryClient();
  const [error, setError] = useState<unknown>(undefined);
  const [isPending, setIsPending] = useState(false);
  const { t: translate } = useTranslation(["common", "dags"]);

  const invalidateQueries = async () => {
    await Promise.all([
      queryClient.invalidateQueries({ queryKey: [useDagRunServiceGetDagRunsKey] }),
      queryClient.invalidateQueries({ queryKey: [useTaskInstanceServiceGetTaskInstancesKey] }),
    ]);
  };

  const handleResult = (
    actionResult: BulkActionResponse | null | undefined,
    toasterKey: ToasterKey,
  ): boolean => {
    const { firstErrorDetail, successCount, successKeys } = formatActionResult(actionResult);

    if (successCount > 0) {
      toaster.create({
        description: translate(`${toasterKey}.success.description`, {
          count: successCount,
          keys: successKeys.join(", "),
          resourceName: translate("dagRun_other"),
        }),
        title: translate(`${toasterKey}.success.title`, {
          resourceName: translate("dagRun_other"),
        }),
        type: "success",
      });
    }

    if (firstErrorDetail !== null) {
      setError({ body: { detail: firstErrorDetail } });

      return false;
    }

    setError(undefined);

    return true;
  };

  const bulkClear = async (dagRuns: Array<DAGRunResponse>, options: BulkClearDagRunsOptions) => {
    setError(undefined);
    setIsPending(true);

    try {
      const response = await DagRunService.postClearDagRuns({
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

      await invalidateQueries();

      if (handleResult(response, "toaster.bulkClear")) {
        clearSelections();
        onSuccessConfirm();
      }
    } catch (_error) {
      setError(_error);
    }
    setIsPending(false);
  };

  const handleBulkResponse = (
    response: BulkResponse,
    toasterKey: "toaster.bulkDelete" | "toaster.bulkUpdate",
  ) => {
    const actionResult = toasterKey === "toaster.bulkDelete" ? response.delete : response.update;

    if (handleResult(actionResult, toasterKey)) {
      clearSelections();
      onSuccessConfirm();
    }
  };

  const bulkDelete = async (dagRuns: Array<DAGRunResponse>) => {
    setError(undefined);
    setIsPending(true);

    try {
      const response = await DagRunService.bulkDagRuns({
        dagId: "~",
        requestBody: {
          actions: [
            {
              action: "delete" as const,
              action_on_non_existence: "skip",
              entities: dagRuns.map((dr) => ({
                dag_id: dr.dag_id,
                dag_run_id: dr.dag_run_id,
              })),
            },
          ],
        },
      });

      await invalidateQueries();
      handleBulkResponse(response, "toaster.bulkDelete");
    } catch (_error) {
      setError(_error);
    }
    setIsPending(false);
  };

  const bulkMarkAs = async (dagRuns: Array<DAGRunResponse>, options: BulkMarkOptions) => {
    setError(undefined);
    setIsPending(true);

    const updateMask = options.note === null ? ["state"] : ["state", "note"];

    try {
      const response = await DagRunService.bulkDagRuns({
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

      await invalidateQueries();
      handleBulkResponse(response, "toaster.bulkUpdate");
    } catch (_error) {
      setError(_error);
    }
    setIsPending(false);
  };

  return { bulkClear, bulkDelete, bulkMarkAs, error, isPending, setError };
};
