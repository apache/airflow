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
  useDagRunServiceClearDagRuns,
  UseDagRunServiceGetDagRunKeyFn,
  useDagRunServiceGetDagRunsKey,
  UseGanttServiceGetGanttDataKeyFn,
  useTaskInstanceServiceGetMappedTaskInstanceKey,
  useTaskInstanceServiceGetTaskInstanceKey,
  useTaskInstanceServiceGetTaskInstancesKey,
} from "openapi/queries";
import type { ClearDagRunsResponse, DAGRunResponse } from "openapi/requests/types.gen";
import { toaster } from "src/components/ui";

import { gridQueryKeys, tiPerAttemptQueryKeys } from "./gridViewQueryKeys";
import { useBulkClearDagRunsDryRunKey } from "./useBulkClearDagRunsDryRun";
import { useClearDagRunDryRunKey } from "./useClearDagRunDryRun";

type Props = {
  readonly deselectKeys: (keys: Array<string>) => void;
  readonly onSuccessConfirm: VoidFunction;
};

export type BulkClearDagRunsOptions = {
  note: string | null;
  onlyFailed: boolean;
  onlyNew: boolean;
};

export const useBulkClearDagRuns = ({ deselectKeys, onSuccessConfirm }: Props) => {
  const queryClient = useQueryClient();
  const { t: translate } = useTranslation(["common", "dags"]);

  const onSuccess = async (responseData: ClearDagRunsResponse) => {
    const clearedRuns = "dag_runs" in responseData ? [...responseData.dag_runs] : [];
    const dagIds = new Set(clearedRuns.map((dagRun) => dagRun.dag_id));

    const keys = [
      [useDagRunServiceGetDagRunsKey],
      [useTaskInstanceServiceGetTaskInstancesKey],
      [useTaskInstanceServiceGetTaskInstanceKey],
      [useTaskInstanceServiceGetMappedTaskInstanceKey],
      [useBulkClearDagRunsDryRunKey],
      ...tiPerAttemptQueryKeys,
      ...[...dagIds].flatMap((dagId) => [...gridQueryKeys(dagId), [useClearDagRunDryRunKey, dagId]]),
      ...clearedRuns.flatMap((dagRun) => [
        UseDagRunServiceGetDagRunKeyFn({ dagId: dagRun.dag_id, dagRunId: dagRun.dag_run_id }),
        UseGanttServiceGetGanttDataKeyFn({ dagId: dagRun.dag_id, runId: dagRun.dag_run_id }),
      ]),
    ];

    await Promise.all(keys.map((queryKey) => queryClient.invalidateQueries({ queryKey })));

    toaster.create({
      description: translate("toaster.bulkClear.success.description", {
        count: clearedRuns.length,
        keys: clearedRuns.map((dagRun) => dagRun.dag_run_id).join(", "),
        resourceName: translate("dagRun_other"),
      }),
      title: translate("toaster.bulkClear.success.title", {
        resourceName: translate("dagRun_other"),
      }),
      type: "success",
    });
    deselectKeys(clearedRuns.map((dagRun) => `${dagRun.dag_id}.${dagRun.dag_run_id}`));
    onSuccessConfirm();
  };

  const clearDagRuns = useDagRunServiceClearDagRuns({ onSuccess });

  const bulkClear = (dagRuns: Array<DAGRunResponse>, options: BulkClearDagRunsOptions) => {
    clearDagRuns.reset();
    clearDagRuns.mutate({
      dagId: "~",
      requestBody: {
        dag_runs: dagRuns.map((dagRun) => ({ dag_id: dagRun.dag_id, dag_run_id: dagRun.dag_run_id })),
        dry_run: false,
        note: options.note ?? undefined,
        only_failed: options.onlyFailed,
        only_new: options.onlyNew,
      },
    });
  };

  return {
    bulkClear,
    error: clearDagRuns.error,
    isPending: clearDagRuns.isPending,
    reset: clearDagRuns.reset,
  };
};
