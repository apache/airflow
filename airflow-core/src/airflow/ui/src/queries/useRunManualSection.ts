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
  UseDagRunServiceGetDagRunKeyFn,
  useDagRunServiceGetDagRunsKey,
  UseGanttServiceGetGanttDataKeyFn,
  UseTaskInstanceServiceGetTaskInstanceKeyFn,
  useTaskInstanceServiceGetTaskInstancesKey,
  useTaskInstanceServiceRunManualSection,
} from "openapi/queries";
import type { RunManualSectionBody, RunManualSectionResponse } from "openapi/requests/types.gen";
import { createErrorToaster } from "src/utils";

import { gridQueryKeys, tiPerAttemptQueryKeys } from "./gridViewQueryKeys";
import { useClearTaskInstancesDryRunKey } from "./useClearTaskInstancesDryRun";
import { usePatchTaskInstanceDryRunKey } from "./usePatchTaskInstanceDryRun";
import { useRunManualSectionDryRunKey } from "./useRunManualSectionDryRun";

export const useRunManualSection = ({
  dagId,
  dagRunId,
  onSuccess,
  taskId,
}: {
  dagId: string;
  dagRunId: string;
  onSuccess: () => void;
  taskId: string;
}) => {
  const queryClient = useQueryClient();
  const { t: translate } = useTranslation();

  const onError = (error: unknown) => {
    createErrorToaster(
      error,
      {
        params: { resourceName: translate("dags:runAndTaskActions.manualSection.resourceName") },
        titleKey: "toaster.update.error",
      },
      translate,
    );
  };

  const onSuccessFn = async (
    _: RunManualSectionResponse,
    variables: {
      dagId: string;
      dagRunId: string;
      requestBody: RunManualSectionBody;
      taskId: string;
    },
  ) => {
    const queryKeys = [
      UseTaskInstanceServiceGetTaskInstanceKeyFn({ dagId, dagRunId, taskId }),
      UseDagRunServiceGetDagRunKeyFn({ dagId, dagRunId }),
      [useDagRunServiceGetDagRunsKey],
      [useTaskInstanceServiceGetTaskInstancesKey],
      [useRunManualSectionDryRunKey, dagId, dagRunId, taskId],
      [useClearTaskInstancesDryRunKey, dagId],
      [usePatchTaskInstanceDryRunKey, dagId, dagRunId],
      UseGanttServiceGetGanttDataKeyFn({ dagId, runId: dagRunId }),
      ...tiPerAttemptQueryKeys,
    ];

    await Promise.all([
      ...gridQueryKeys(variables.dagId).map((key) => queryClient.invalidateQueries({ queryKey: key })),
      ...queryKeys.map((key) => queryClient.invalidateQueries({ queryKey: key })),
    ]);

    onSuccess();
  };

  return useTaskInstanceServiceRunManualSection({
    onError,
    onSuccess: onSuccessFn,
  });
};
