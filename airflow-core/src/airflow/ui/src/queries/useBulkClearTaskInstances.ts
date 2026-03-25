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
import { TaskInstanceService } from "openapi/requests/services.gen";
import type { TaskInstanceResponse } from "openapi/requests/types.gen";
import { toaster } from "src/components/ui";

type Props = {
  readonly clearSelections: VoidFunction;
  readonly onSuccessConfirm: VoidFunction;
};

export type BulkClearOptions = {
  includeDownstream: boolean;
  includeFuture: boolean;
  includeOnlyFailed: boolean;
  includePast: boolean;
  includeUpstream: boolean;
  note: string | null;
  preventRunningTask: boolean;
};

export const useBulkClearTaskInstances = ({ clearSelections, onSuccessConfirm }: Props) => {
  const queryClient = useQueryClient();
  const [error, setError] = useState<unknown>(undefined);
  const [isPending, setIsPending] = useState(false);
  const { t: translate } = useTranslation(["common", "dags"]);

  const invalidateQueries = async () => {
    await Promise.all([
      queryClient.invalidateQueries({ queryKey: [useTaskInstanceServiceGetTaskInstancesKey] }),
      queryClient.invalidateQueries({ queryKey: [useDagRunServiceGetDagRunsKey] }),
    ]);
  };

  const bulkClear = async (taskInstances: Array<TaskInstanceResponse>, options: BulkClearOptions) => {
    setError(undefined);
    setIsPending(true);

    // Group by (dag_id, dag_run_id) — clear endpoint requires a specific dag_id
    // and dag_run_id scopes the clear to the specific run
    const byDagRun = new Map<string, { dagId: string; dagRunId: string; tis: Array<TaskInstanceResponse> }>();

    for (const ti of taskInstances) {
      const key = `${ti.dag_id}::${ti.dag_run_id}`;
      const group = byDagRun.get(key) ?? { dagId: ti.dag_id, dagRunId: ti.dag_run_id, tis: [] };

      group.tis.push(ti);
      byDagRun.set(key, group);
    }

    try {
      await Promise.all(
        [...byDagRun.values()].map(({ dagId, dagRunId, tis }) =>
          TaskInstanceService.postClearTaskInstances({
            dagId,
            requestBody: {
              dag_run_id: dagRunId,
              dry_run: false,
              include_downstream: options.includeDownstream,
              include_future: options.includeFuture,
              include_past: options.includePast,
              include_upstream: options.includeUpstream,
              note: options.note,
              only_failed: options.includeOnlyFailed,
              ...(options.preventRunningTask ? { prevent_running_task: true } : {}),
              task_ids: tis.map((ti) =>
                ti.map_index >= 0 ? ([ti.task_id, ti.map_index] as [string, number]) : ti.task_id,
              ),
            },
          }),
        ),
      );

      await invalidateQueries();

      toaster.create({
        description: translate("toaster.bulkClear.success.description", {
          count: taskInstances.length,
          keys: taskInstances.map((ti) => ti.task_id).join(", "),
          resourceName: translate("taskInstance_other"),
        }),
        title: translate("toaster.bulkClear.success.title", {
          resourceName: translate("taskInstance_other"),
        }),
        type: "success",
      });

      clearSelections();
      onSuccessConfirm();
    } catch (_error) {
      setError(_error);
    }
    setIsPending(false);
  };

  return { bulkClear, error, isPending, setError };
};
