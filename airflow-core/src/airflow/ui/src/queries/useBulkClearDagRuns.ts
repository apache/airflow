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
  UseDagRunServiceGetDagRunKeyFn,
  useDagRunServiceGetDagRunsKey,
  UseGanttServiceGetGanttDataKeyFn,
  useTaskInstanceServiceGetMappedTaskInstanceKey,
  useTaskInstanceServiceGetTaskInstanceKey,
  useTaskInstanceServiceGetTaskInstancesKey,
} from "openapi/queries";
import { DagRunService } from "openapi/requests/services.gen";
import type { BulkActionResponse, DAGRunResponse } from "openapi/requests/types.gen";
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

// Mirrors the bulk-endpoint success key (``{dag_id}.{run_id}``) so callers can pass
// the result straight into ``deselectKeys`` without an extra mapping.
const getRowKey = (dagRun: DAGRunResponse) => `${dagRun.dag_id}.${dagRun.dag_run_id}`;

const formatError = (reason: unknown): string => {
  if (reason instanceof Error) {
    return reason.message;
  }
  if (typeof reason === "object" && reason !== null && "body" in reason) {
    const { body } = reason as { body?: { detail?: unknown } };

    if (body?.detail !== undefined) {
      return typeof body.detail === "string" ? body.detail : JSON.stringify(body.detail);
    }
  }

  return String(reason);
};

export const useBulkClearDagRuns = ({ deselectKeys, onSuccessConfirm }: Props) => {
  const queryClient = useQueryClient();
  const [data, setData] = useState<{ clear: BulkActionResponse } | undefined>(undefined);
  const [isPending, setIsPending] = useState(false);
  const { t: translate } = useTranslation(["common", "dags"]);

  const reset = () => {
    setData(undefined);
  };

  const invalidateQueries = async (dagRuns: ReadonlyArray<DAGRunResponse>) => {
    const dagIds = new Set(dagRuns.map((dagRun) => dagRun.dag_id));
    const keys = [
      [useDagRunServiceGetDagRunsKey],
      [useTaskInstanceServiceGetTaskInstancesKey],
      [useTaskInstanceServiceGetTaskInstanceKey],
      [useTaskInstanceServiceGetMappedTaskInstanceKey],
      [useBulkClearDagRunsDryRunKey],
      ...tiPerAttemptQueryKeys,
      ...[...dagIds].flatMap((dagId) => [...gridQueryKeys(dagId), [useClearDagRunDryRunKey, dagId]]),
      ...dagRuns.flatMap((dagRun) => [
        UseDagRunServiceGetDagRunKeyFn({ dagId: dagRun.dag_id, dagRunId: dagRun.dag_run_id }),
        UseGanttServiceGetGanttDataKeyFn({ dagId: dagRun.dag_id, runId: dagRun.dag_run_id }),
      ]),
    ];

    await Promise.all(keys.map((queryKey) => queryClient.invalidateQueries({ queryKey })));
  };

  const bulkClear = async (dagRuns: Array<DAGRunResponse>, options: BulkClearDagRunsOptions) => {
    reset();
    setIsPending(true);

    const settled = await Promise.allSettled(
      dagRuns.map((dagRun) =>
        DagRunService.clearDagRun({
          dagId: dagRun.dag_id,
          dagRunId: dagRun.dag_run_id,
          requestBody: {
            dry_run: false,
            note: options.note ?? undefined,
            only_failed: options.onlyFailed,
            only_new: options.onlyNew,
          },
        }).then(() => dagRun),
      ),
    );

    const succeeded: Array<DAGRunResponse> = [];
    const errors: Array<Record<string, unknown>> = [];

    settled.forEach((outcome, index) => {
      if (outcome.status === "fulfilled") {
        succeeded.push(outcome.value);
      } else {
        const dagRun = dagRuns[index];

        errors.push({
          error: dagRun
            ? `${getRowKey(dagRun)}: ${formatError(outcome.reason)}`
            : formatError(outcome.reason),
        });
      }
    });

    await invalidateQueries(dagRuns);

    if (succeeded.length > 0) {
      toaster.create({
        description: translate("toaster.bulkClear.success.description", {
          count: succeeded.length,
          keys: succeeded.map((dagRun) => dagRun.dag_run_id).join(", "),
          resourceName: translate("dagRun_other"),
        }),
        title: translate("toaster.bulkClear.success.title", {
          resourceName: translate("dagRun_other"),
        }),
        type: "success",
      });
      deselectKeys(succeeded.map(getRowKey));
    }

    setData({ clear: { errors, success: succeeded.map(getRowKey) } });
    setIsPending(false);

    // Per-run failures keep the dialog open so the user can see what failed;
    // the consumer renders ``data.clear.errors``.
    if (errors.length === 0) {
      onSuccessConfirm();
    }
  };

  return { bulkClear, data, isPending, reset };
};
