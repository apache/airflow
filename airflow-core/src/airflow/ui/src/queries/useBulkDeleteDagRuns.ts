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
  useDagRunServiceDeleteDagRun,
  useDagRunServiceGetDagRunsKey,
  UseDagRunServiceGetDagRunKeyFn,
  useTaskInstanceServiceGetTaskInstancesKey,
  useTaskInstanceServiceGetHitlDetailsKey,
} from "openapi/queries";
import { toaster } from "src/components/ui";

export type SelectedRun = { dagId: string; dagRunId: string };

export const useBulkDeleteDagRuns = (onSuccessConfirm?: () => void) => {
  const { t: translate } = useTranslation();
  const queryClient = useQueryClient();

  const deleteMutation = useDagRunServiceDeleteDagRun();

  const bulkDelete = async (runs: Array<SelectedRun>): Promise<void> => {
    if (runs.length === 0) {
      return;
    }

    const results = await Promise.allSettled(
      runs.map(({ dagId, dagRunId }) => deleteMutation.mutateAsync({ dagId, dagRunId })),
    );

    const failed = results.filter((result) => result.status === "rejected");

    const queryKeys = [
      [useDagRunServiceGetDagRunsKey],
      [useTaskInstanceServiceGetTaskInstancesKey],
      [useTaskInstanceServiceGetHitlDetailsKey],
      ...runs.map(({ dagId, dagRunId }) => UseDagRunServiceGetDagRunKeyFn({ dagId, dagRunId })),
    ];

    await Promise.all(queryKeys.map((key) => queryClient.invalidateQueries({ queryKey: key })));

    if (failed.length > 0) {
      toaster.create({
        description: `${failed.length}/${runs.length} failed`,
        title: translate("dags:runAndTaskActions.delete.error", { type: translate("dagRun_one") }),
        type: "error",
      });

      return;
    }

    toaster.create({
      description: translate("dags:runAndTaskActions.delete.success.description", {
        type: translate("dagRun_one"),
      }),
      title: translate("dags:runAndTaskActions.delete.success.title", { type: translate("dagRun_one") }),
      type: "success",
    });

    onSuccessConfirm?.();
  };

  return {
    bulkDelete,
    isDeleting: deleteMutation.isPending,
  };
};
